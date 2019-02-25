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