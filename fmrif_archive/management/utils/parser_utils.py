import gzip
import warnings
import base64
import pydicom
import rapidjson as json
import os
import shutil
import traceback

from multiprocessing.dummy import Pool as ThreadPool  # Use threads
from multiprocessing import Pool as ProcessPool
from subprocess import check_output
from pydicom.errors import InvalidDicomError
from subprocess import STDOUT, run, CalledProcessError, PIPE
from pathlib import Path
from collections import OrderedDict
from Crypto.Hash import SHA512


# Load the functions to read CSA Headers and ignore the warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=UserWarning)
    from nibabel.nicom.csareader import get_csa_header, is_mosaic, CSAError, CSAReadError


class UtilsLogger:

    def __init__(self, log=None):
        self.log = log

    def info(self, msg):

        if self.log:
            self.log.info(msg)
        else:
            print(msg)

    def warning(self, msg):

        if self.log:
            self.log.warning(msg)
        else:
            print(msg)

    def debug(self, msg):

        if self.log:
            self.log.debug(msg)
        else:
            print(msg)

    def error(self, msg):

        if self.log:
            self.log.error(msg)
        else:
            print(msg)


def parse_date(date):
    if date != "*":
        if date.isdigit():
            if len(date) == 1:
                return "0{}".format(date)
            else:
                return date
        else:
            raise ValueError("Date selection must be integers (as 1 or 2 digits for day "
                             "and month, and 4 digit year)")
    return date


def sanitize_unicode(s):
    """Removes any \u0000 characters from unicode strings in DICOM values, since this character is
    unsupported in JSON"""

    if type(s) is bytes:
        s = s.decode('utf-8')
    return str(s).replace(u"\u0000", "").strip()


def parse_pn(value):
    """Parses a Person Name (VR of type PN) DICOM value into the appropriate JSON Model Object representation"""

    pn = OrderedDict({
        'Alphabetic': str(value)
    })

    if value.ideographic:
        pn["Ideographic"] = value.ideographic

    if value.phonetic:
        pn["Phonetic"] = value.phonetic

    return pn


def parse_seq(seq):
    """Parses a sequence (VR of type SQ) of DICOM values into the appropriate JSON Model Object representation"""

    vals = []

    for element in seq.value:
        if element:
            vals.append(parse_dicom_dataset(element))
        else:
            vals.append(None)
    return vals


def parse_at(value):
    return str(value).replace("(", "").replace(")", "").replace(", ", "")


def parse_ui(value):
    return str(repr(value).replace('"', '').replace("'", ""))


def _vr_encoding(vr):
    """A map of DICOM VRs to corresponding JSON types as specified in the DICOMweb standard"""

    vr_json_encodings = {
        'AE': sanitize_unicode,
        'AS': sanitize_unicode,
        'AT': parse_at,
        'CS': sanitize_unicode,
        'DA': sanitize_unicode,
        'DS': float,
        'DT': sanitize_unicode,
        'FL': float,
        'FD': float,
        'IS': int,
        'LO': sanitize_unicode,
        'LT': sanitize_unicode,
        'PN': parse_pn,
        'SH': sanitize_unicode,
        'SL': int,
        'SS': int,
        'ST': sanitize_unicode,
        'TM': sanitize_unicode,
        'UC': sanitize_unicode,
        'UI': parse_ui,
        'UL': int,
        'UR': sanitize_unicode,
        'US': int,
        'UT': sanitize_unicode
    }

    return vr_json_encodings[vr]


def encode_element(dicom_element):
    """Creates the appropriate JSON Model Object representation for a DICOM element"""

    if dicom_element.VR == 'SQ':

        return OrderedDict({
            'vr': dicom_element.VR,
            'Value': parse_seq(dicom_element)
        })

    elif pydicom.dataelem.isMultiValue(dicom_element.value):

        vals = []

        for val in dicom_element.value:
            if val != '' and val is not None:
                vals.append(_vr_encoding(dicom_element.VR)(val))
            else:
                vals.append(None)

        return OrderedDict({
            'vr': dicom_element.VR,
            'Value': vals
        })

    elif type(dicom_element.value) == pydicom.dataset.Dataset:

        return OrderedDict({
            'vr': dicom_element.VR,
            'Value': [parse_dicom_dataset(dicom_element.value)]
        })

    else:

        model_obj = OrderedDict({
            'vr': dicom_element.VR
        })

        # The conditions inside the or clause needed because numeric values of 0 will fail the
        # first part of the if clause. We really only want to exclude empty strings.
        if dicom_element.value or (dicom_element.value != '' and dicom_element.value != b''):
            model_obj["Value"] = [_vr_encoding(dicom_element.VR)(dicom_element.value)]

        return model_obj


def parse_dicom_dataset(dicom_dataset):
    """Parses a DICOM dataset and converts the DICOM elements into their appropriate JSON Model Object
    representations"""

    dicom_dict = OrderedDict()

    for dicom_element in dicom_dataset:

        tag = "".join(str(dicom_element.tag).lstrip("(").rstrip(")").split(", ")).upper()

        if dicom_element.VR in ('OB', 'OD', 'OF', 'OL', 'OW', 'UN'):

            dicom_dict[tag] = OrderedDict({
                'vr': dicom_element.VR,
                'Available': True
            })

        else:

            dicom_dict[tag] = encode_element(dicom_element)

    return dicom_dict


def decode_ge_private_data(byte_seq):
    """Attempts to uncompress the data in the (0x0025, 0x101B) field of GE Scans, which contains
    useful metadata for DTI scans"""

    # Find the beginning of the GZIP sequence, and drop the padding bytes before it
    pos = byte_seq.find(b"\x1f\x8b")

    if pos == -1:

        return None

    else:

        try:

            uncompressed_bytes = gzip.decompress(byte_seq[pos:])

            decoded_bytes = uncompressed_bytes.decode('ascii').strip().split("\n")

            private_dat = OrderedDict()

            for item in decoded_bytes:

                first_space = item.find(" ")

                key = item[:first_space + 1].strip()

                val = item[first_space:].replace('"', '').strip()

                private_dat[key] = val

            return private_dat

        except (ValueError, OSError, TypeError, AttributeError, KeyError):

            pass

    return None


def parse_private_data(dicom_dataset):

    private_data = {
        'is_mosaic': False,
        'data': None
    }

    try:

        manufacturer = dicom_dataset[(0x0008, 0x0070)].value

        if "siemens" in manufacturer.lower():

            siemens_header = get_csa_header(dicom_dataset)

            if siemens_header:

                private_data['is_mosaic'] = is_mosaic(siemens_header)

                # Remove an unused key that is composed of random bytes and seems to be used for padding
                # (can't be converted to json easily)
                siemens_header.pop('unused0', None)

                # # For the remember of the entries, check that the values in the 'items' key of the 'tags'
                # # field in the header are of the proper type according to the specified VR for that tag.
                # # Additionally, ignore tags that are of VR SQ, PN, or any of the binary types.
                if siemens_header.get('tags', None):

                    for tag, val in siemens_header['tags'].items():

                        vr = val['vr']

                        if vr in ('OB', 'OD', 'OF', 'OL', 'OW', 'UN', 'SQ', 'PN'):
                            continue

                        sanitized_items = []

                        for item in val['items']:
                            sanitized_items.append(_vr_encoding(vr)(item))

                        siemens_header['tags'][tag]['items'] = sanitized_items

                # Make sure the remaining csa headers are json serializable
                try:

                    _ = json.dumps(siemens_header)

                    private_data['data'] = siemens_header

                except TypeError:

                    pass

        elif ("ge" in manufacturer.lower()) or ("general electric" in manufacturer.lower()):

            ge_dat = dicom_dataset[(0x0025, 0x101B)].value

            ge_priv_data = decode_ge_private_data(ge_dat)

            if ge_priv_data:
                private_data['data'] = ge_priv_data

    except KeyError:

        pass

    except (CSAError, CSAReadError):

        pass

    return private_data


def parse_json_pn(alphabetic_pn):

    if not alphabetic_pn:
        return None

    try:
        pn = alphabetic_pn.split("^")
    except KeyError:
        return None

    res = {
        'family_name': '',
        'given_name': '',
        'middle_name': '',
        'prefix': '',
        'suffix': ''
    }

    try:
        res['family_name'] = pn[0]
    except IndexError:
        return None

    try:
        res['given_name'] = pn[1]
    except IndexError:
        pass

    try:
        res['middle_name'] = pn[2]
    except IndexError:
        pass

    try:
        res['prefix'] = pn[3]
    except IndexError:
        pass

    try:
        res['suffix'] = pn[4]
    except IndexError:
        pass

    return res


def get_checksum(fpath, algorithm="md5", log=None):

    cmd = "{}sum {}".format(algorithm, fpath)

    try:
        res = run(cmd, check=True, shell=True, universal_newlines=True, stderr=STDOUT, stdout=PIPE)
        checksum = res.stdout.split(" ")[0]
    except CalledProcessError as e:
        checksum = None

        if log:
            log.error("Error computing checksum for file {}\n".format(fpath))
            log.error(e)
            log.error(traceback.format_exc())

    return checksum


def get_exam_id(checksum, fpath):

    fpath = Path(fpath)

    # The filepath for the exam_id is computed from the <scanner> level directory onwards
    fname = fpath.name

    parents = fpath.parents

    exam_subdir = parents[0].name
    day = parents[1].name
    month = parents[2].name
    year = parents[3].name
    scanner = parents[4].name

    id_fpath = "{}/{}/{}/{}/{}/{}".format(scanner, year, month, day, exam_subdir, fname)

    msg = str(checksum) + id_fpath
    enc = base64.b64encode(msg.encode('utf-8'))
    h = SHA512.new(truncate="256")
    h.update(enc)

    return h.hexdigest()


def get_scan_id(exam_id, scan_name):

    msg = str(exam_id) + str(scan_name)
    enc = base64.b64encode(msg.encode('utf-8'))
    h = SHA512.new(truncate="256")
    h.update(enc)
    return h.hexdigest()


def get_scan_checksums(cmd):
    checksum_cmd = cmd[0]
    scan = cmd[1]

    try:
        run(checksum_cmd, check=True, shell=True, cwd=str(scan))
        msg = "Computing checksums for scan {}".format(scan)
    except CalledProcessError:
        msg = "Could not generate checksums for scan {}".format(scan)

    return msg


def _multithreaded_tgz_extraction(filepath, exam_checksum, exam_id, settings):
    """Uncompresses a TGZ image archive from Gold into the specified work directory, or inside a temporary directory
     with randomly generated name within the work directory"""

    compressed_file = Path(filepath)

    parents = compressed_file.parents

    day = parents[1].name
    month = parents[2].name
    year = parents[3].name
    scanner = parents[4].name

    extract_dir = settings['work_dir'] / scanner / year / month / day / exam_id

    if not extract_dir.is_dir():
        extract_dir.mkdir(parents=True, exist_ok=True)

    cmd = "unpigz --keep < {} | tar -xC {}".format(compressed_file, extract_dir)

    try:
        check_output(cmd, stderr=STDOUT, shell=True)
        success = True
    except CalledProcessError:
        success = False

    if success:
        return success, extract_dir, compressed_file, exam_id, exam_checksum
    else:
        return success, None, compressed_file, exam_id, exam_checksum


def _get_dicom_meta(dcm, ge_extra_meta, log):

    dcm = Path(dcm)

    try:
        dicom_dataset = pydicom.dcmread(str(dcm), stop_before_pixels=True)
    except (InvalidDicomError, IOError, OSError) as e:
        log.error("Unable to read: {}".format(str(dcm)))
        log.error(e)
        log.error(traceback.format_exc())
        return dcm, {'sop_instance_uid': None}

    if ge_extra_meta:

        dicom_data = {
            'echo_number': None,
            'raw_data_run_number': None,
            'image_position_patient': None,
            'sop_instance_uid': None,
        }

        try:
            echo_number = encode_element(dicom_dataset[(0x0018, 0x0086)])['Value'][0]
        except (KeyError, TypeError, AttributeError):
            echo_number = None

        try:
            raw_data_run_number = encode_element(dicom_dataset[(0x0019, 0x10A2)])['Value'][0]
        except (KeyError, TypeError, AttributeError):
            raw_data_run_number = None

        try:
            image_position_patient = encode_element(dicom_dataset[(0x0020, 0x0032)])['Value']
        except (KeyError, TypeError, AttributeError):
            image_position_patient = None

        try:
            sop_instance_uid = encode_element(dicom_dataset[(0x0008, 0x0018)])['Value'][0]
        except (KeyError, TypeError, AttributeError):
            sop_instance_uid = None

        dicom_data['echo_number'] = echo_number
        dicom_data['raw_data_run_number'] = raw_data_run_number
        dicom_data['image_position_patient'] = image_position_patient
        dicom_data['sop_instance_uid'] = sop_instance_uid

    else:

        dicom_data = {
            'sop_instance_uid': None,
        }

        try:
            sop_instance_uid = encode_element(dicom_dataset[(0x0008, 0x0018)])['Value'][0]
        except (KeyError, TypeError, AttributeError):
            sop_instance_uid = None

        dicom_data['sop_instance_uid'] = sop_instance_uid

    return dcm, dicom_data


def parse_metadata(extracted_archives, parser_version, log=None):

    log = UtilsLogger(log=log)

    # extracted_archives is a list of tuples of the form
    # (extract_dir, compressed_file, exam_id, exam_checksum)

    for extract_dir, compressed_file, exam_id, exam_checksum in extracted_archives:

        extract_dir = Path(extract_dir)
        compressed_file = Path(compressed_file)

        if not extract_dir.is_dir():

            log.error("Extraction directory {} for compressed {} file is missing. "
                      "Skipping DICOM parsing.".format(extract_dir, compressed_file))

            continue

        if not compressed_file.is_file():

            log.error("Original compressed file is missing: {}. "
                      "Skipping DICOM parsing.".format(compressed_file))

            log.error("Removing extracted directory: {}".format(extract_dir))

            shutil.rmtree(str(extract_dir))

            continue

        session_dirs = list([session for session in Path(extract_dir).glob("*/*") if session.is_dir()])

        if len(session_dirs) != 1:

            log.error("Invalid number of session directories for exam {}. "
                      "Skipping DICOM parsing.".format(compressed_file))

            if Path(extract_dir).is_dir():

                log.error("Removing extracted archive: {}".format(extract_dir))

                shutil.rmtree(str(extract_dir))

            continue

        exam_dir = session_dirs[0]

        scans = [s for s in exam_dir.iterdir() if s.is_dir()]

        if not len(scans) > 0:

            log.error("No scans found in exam {}".format(compressed_file))

            log.error("Removing extracted archive: {}".format(extract_dir))

            shutil.rmtree(str(extract_dir))

            continue

        study_meta = OrderedDict({
            'metadata': OrderedDict({
                'exam_id': exam_id,
                'gold_fpath': "/".join(str(compressed_file).split("/")[-6:]),
                'gold_archive_checksum': exam_checksum,
                'parser_version': parser_version,
            }),
            'data': [],
        })

        for scan in scans:

            scan_id = get_scan_id(exam_id, scan.name)

            instance_files = [f for f in scan.iterdir() if f.is_file() and "README" not in f.name]

            if not instance_files:

                log.error("No instances files found in subdirectory {}".format(scan))

                continue

            scan_outfname = "{}_{}_scan_{}_metadata.txt".format(
                str(compressed_file.name).replace(".tgz", ""),
                exam_id,
                scan.name
            )

            scan_meta = OrderedDict({
                'metadata': {
                    'parent_exam_id': exam_id,
                    'gold_scan_dir': scan.name,
                    'scan_id': scan_id,
                    'num_files': len(instance_files),
                    'parser_version': parser_version,
                },
                'dicom_data': None,
                'private_data': None,
            })

            dicom_instances = [i for i in instance_files if i.name.endswith(".dcm")]

            if not dicom_instances:

                # There are files in the subdirectory, but none of them are DICOM.
                # Save basic metadata about directory

                study_meta['data'].append(scan_meta)

                continue

            # There are dicom instances - try to open at least one of them to get
            # basic metadata for this scan
            sample_file = None

            for dcm_instance in dicom_instances:

                try:

                    sample_file = pydicom.dcmread(str(dcm_instance), stop_before_pixels=True)

                    break

                except (InvalidDicomError, IOError, OSError):

                    log.error("Unable to open invalid DICOM file {}".format(sample_file))

                    sample_file = None

            if not sample_file:

                # None of the DICOM files in subirectory was readable, log an error
                # and add basic subdirectory metadata to study metadata file

                study_meta['data'].append(scan_meta)

                log.error("Unable to open any DICOMs for scan {}".format(scan))

                continue

            log.info("Found {} DICOM files for scan {} of exam {}".format(
                len(instance_files),
                scan.name,
                exam_dir)
            )

            dicom_data = parse_dicom_dataset(sample_file)

            scan_meta['dicom_data'] = parse_dicom_dataset(sample_file)

            scan_meta['private_data'] = parse_private_data(sample_file)

            study_meta['data'].append(scan_meta)

            collect_ge_extra_meta = False

            try:
                sop_class = dicom_data['00080016']['Value'][0]
            except (KeyError, IndexError):
                sop_class = None

            try:
                manufacturer = dicom_data['00080070']['Value'][0]
            except (KeyError, IndexError):
                manufacturer = None

            if sop_class and manufacturer:

                if (sop_class in ["1.2.840.10008.5.1.4.1.1.4", "1.2.840.10008.5.1.4.1.1.4.1"]) and \
                        ("ge" in manufacturer.lower() or "general electric" in manufacturer.lower()):

                    # Determine if series is likely to be multiecho and if so, collect the relevant metadata
                    # to order the scans
                    try:
                        num_indices = dicom_data["00201002"]['Value'][0]
                    except (KeyError, IndexError):
                        num_indices = None

                    try:
                        num_slices = dicom_data["0021104F"]['Value'][0]
                    except (KeyError, IndexError):
                        num_slices = None

                    if not num_indices or not num_slices:
                        log.info("Scan did not have number of slices or number of indices in metadata. Treating"
                                 " as non-multiecho.")
                    else:

                        log.info("Num Indices: {}".format(num_indices))
                        log.info("Num Slices: {}".format(num_slices))

                        if num_indices != num_slices:

                            if num_indices % num_slices != 0:
                                log.warning("Multiecho testing detected possible un-accounted for slices in this "
                                            "acquisition. Treating as a non-multiecho series.")
                            else:

                                num_echoes = num_indices // num_slices

                                log.info("Number of echoes detected: {}".format(num_echoes))

                                # Try to fetch a slice representative slice index - if unable, might be CBV scan
                                try:
                                    sample_slice_index = dicom_data['001910A2']['Value'][0]
                                except (KeyError, IndexError):
                                    sample_slice_index = None

                                if not sample_slice_index:
                                    log.warning("Unable to retrieve slice indices "
                                                "(usually happens with CBV scans), treating "
                                                "as non-multiecho series.")
                                else:

                                    # Collect GE metadata for sorting
                                    log.info("Scan is probable multiecho - collecting extra metadata for sorting")
                                    collect_ge_extra_meta = True

            with open(str(exam_dir / scan_outfname), mode="wt") as outfile:

                num_workers = 32

                with ProcessPool(num_workers) as pool:
                    instance_results = []

                    for dcm, dicom_data in pool.starmap(
                            _get_dicom_meta,
                            [(dcm, collect_ge_extra_meta, log) for dcm in instance_files]
                    ):
                        instance_results.append("{}\t{}".format(
                            Path(dcm).name,
                            json.dumps(dicom_data)
                        ))

                outfile.write("\n".join(instance_results))

        checksum_cmds = []

        for scan in [d for d in exam_dir.iterdir() if d.is_dir()]:

            # Get checksum for files in scan

            checksum_cmd = "touch {}/{}_{}_scan_{}_checksum.txt " \
                           "&& find . -type f | xargs -I {{}} md5sum {{}} " \
                           ">> {}/{}_{}_scan_{}_checksum.txt".format(
                                exam_dir,
                                compressed_file.name.replace(".tgz", ""),
                                exam_id,
                                scan.name,
                                exam_dir,
                                compressed_file.name.replace(".tgz", ""),
                                exam_id,
                                scan.name
                            )

            checksum_cmds.append([checksum_cmd, str(scan)])

        with ProcessPool(16) as pool:

            for msg in pool.imap(get_scan_checksums, checksum_cmds):
                if "Could not" in msg:
                    log.error(msg)
                else:
                    log.info(msg)

        study_outfname = exam_dir / "study_{}_metadata.txt".format(study_meta['metadata']['exam_id'])

        with open(str(study_outfname), "wt") as study_outfile:
            json.dump(study_meta, study_outfile)

        log.info("Removing tmp files...")

        for scan_dir in [d for d in exam_dir.iterdir() if d.is_dir()]:
            shutil.rmtree(scan_dir)

        readme_files = [str(f) for f in exam_dir.glob("**/*") if
                        f.is_file() and ("_metadata.txt" not in f.name) and
                        ("_checksum.txt" not in f.name) and ("_filelist.txt" not in f.name)]

        list(map(os.remove, readme_files))


def uncompress_tgz_files(compressed_files, settings):

    # Note compressed_files is a list of tuples with items (filepath, exam_checksum, exam_id)

    num_workers = settings['tgz_cores']

    extracted_archives = []
    msgs = []

    with ThreadPool(num_workers) as pool:

        for success, extract_dir, compressed_file, exam_id, exam_checksum in pool.starmap(
                _multithreaded_tgz_extraction,
                [(*compressed_file, settings) for compressed_file in compressed_files]
        ):
            if success:
                extracted_archives.append((extract_dir, compressed_file, exam_id, exam_checksum))
                msgs.append("Extracted archive {}".format(compressed_file))
            else:
                msgs.append("Unable to extract archive {}".format(compressed_file))

    return extracted_archives, msgs
