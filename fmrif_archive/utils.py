from collections import OrderedDict
from fmrif_archive.dicom_mappings import DCM_TAG_TO_KWD


def parse_pn(alphabetic_pn):

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


def get_fmrif_scanner(curr_scanner):

    scanners = {
        "fmrif3ta": ["3TaFMRI", "fmrif3ta", "fmri3Ta"],
        "fmrif3tb": ["fmri3Tb", "fmrif3tb"],
        "fmrif3tc": ["fmrif3tc", "fmri3Tc", "DISCOVERY MR750"],
        "fmrif3td": ["AWP45160", "Skyra"],
        "fmrif7t": ["FMRIFD7T", "Investigational_Device_7T", "NMRF7T"]
    }

    for fmrif_scanner, station_names in scanners.items():

        if curr_scanner in station_names:
            return fmrif_scanner

    return None


def dicom_json_to_keyword_and_flatten(dicom_json):

    new_summary = OrderedDict()

    for key, val in dicom_json.items():

        new_key = DCM_TAG_TO_KWD.get(key, None)

        if val['vr'] == 'SQ' and val.get('Value', None):

            old_values = val['Value']
            new_values = []
            for old_val in old_values:
                new_values.append(dicom_json_to_keyword_and_flatten(old_val))

            if new_key:
                new_summary[new_key] = new_values
            else:
                new_summary[key] = new_values

        else:

            if new_key:
                new_summary[new_key] = val.get('Value', None)
            else:
                new_summary[key] = val.get('Value', None)

    return OrderedDict(sorted(new_summary.items()))
