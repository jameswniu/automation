import os, sys
import re

import paramiko

from copy import deepcopy
from string import punctuation

from datetime import datetime as dt
from usps import USPSApi, Address
from paramiko import SSHClient
from scp import SCPClient

from config import user_linux, passwd_linux


def trim_name(name, thres=30):
    try:
        global c0, c1
    except:
        pass

    # remove alias and everything after
    alias = ['DBA', 'LLC', 'INC', 'COMPANY', 'CO', 'CORP', 'CORPORATION', 'ATTN', 'PC', 'C O', "FKA"]
    # abbrev directions
    pot = {
        'EAST': 'E',
        'WEST': 'W',
        'NORTH': 'N',
        'SOUTH': 'S',
        'NORTHEAST': 'NE',
        'NORTHWEST': 'NW',
        'SOUTHEAST': 'SE',
        'SOUTHWEST': 'SW'
    }
    # first approx match dict
    jar = {
        "ALABAMA": "AL"
        , "ALASKA": "AK"
        , "ARIZONA": "AZ"
        , "ARKANSAS": "AR"
        , "ARMED FORCES PACIFIC": "AP"
        , "CALIFORNIA": "CA"
        , "COLORADO": "CO"
        , "CONNECTICUT": "CT"
        , "DELAWARE": "DE"
        , "DISTRICT OF COLUMBIA": "DC"
        , "FLORIDA": "FL"
        , "GEORGIA": "GA"
        , "HAWAII": "HI"
        , "IDAHO": "ID"
        , "ILLINOIS": "IL"
        , "INDIANA": "IN"
        , "IOWA": "IA"
        , "KANSAS": "KS"
        , "KENTUCKY": "KY"
        , "LOUISIANA": "LA"
        , "MAINE": "ME"
        , "MARYLAND": "MD"
        , "MASSACHUSETTS": "MA"
        , "MICHIGAN": "MI"
        , "MINNESOTA": "MN"
        , "MISSISSIPPI": "MS"
        , "MISSOURI": "MO"
        , "MONTANA": "MT"
        , "NEBRASKA": "NE"
        , "NEVADA": "NV"
        , "NEW HAMPSHIRE": "NH"
        , "NEW JERSEY": "NJ"
        , "NEW MEXICO": "NM"
        , "NEW YORK": "NY"
        , "NORTH CAROLINA": "NC"
        , "NORTH DAKOTA": "ND"
        , "OHIO": "OH"
        , "OKLAHOMA": "OK"
        , "OREGON": "OR"
        , "PENNSYLVANIA": "PA"
        , "RHODE ISLAND": "RI"
        , "SOUTH CAROLINA": "SC"
        , "SOUTH DAKOTA": "SD"
        , "TENNESSEE": "TN"
        , "TEXAS": "TX"
        , "UTAH": "UT"
        , "VERMONT": "VT"
        , "VIRGINIA": "VA"
        , "VIRGIN ISLANDS": "VI"
        , "WASHINGTON": "WA"
        , "WEST VIRGINIA": "WV"
        , "WISCONSIN": "WI"
        , "WYOMING": "WY"
        , "CORPORATION": "CORP"
        , "COMPANY": "CO"
        , "INSURANCE": "INS"
        , "LLC": ""
        , "MANAGEMENT": "MGMT"
        , "GROUP": "GRP"
        , "SOLUTION": "SOLN"
        , "SERVICE": "SVC"
        , "EMPLOYMENT": "EMPL"
        , "DEPARTMENT": "DEPT"
        , "COM PANY": ""
        , "SCHOOL": "SCH"
        , "METROPOLITAN ATLANTA RAPID TRANSIT AUTHORITY": "MARTA"
        , "GENERAL PARTNERSHIP": "GP"
        , "LIMITED PARTNERSHIP": "LP"
        , "LIMITED LIABILITY PARTNERSHIP": "LLP"
        , "EMERGENCY": "EMERG"
        , "PARTNER": "PTNR"
        , "CHICAGO TRANSIT AUTHORITY": "CTA"
        , "TECHNOLOGY": "TECH"
        , "TECHNOLOGIES": "TECH"
        , "INCORPORATED": "INC"
        , "CORPORATIO": ""
        , "ASSOCIATION": "ASSOC"
        , "SOLUTION": "SOLN"
        , "CONDOMINIUM": "CONDO"
    }
    # then exact match dict if thres not satisfied
    jar1 = {
        "HLDG": ""
        , "GRP": ""
        , "CORP": ""
        , "CO": ""
        , "LTD": ""
        , "INC": ""
        , "LLP": ""
        , "LP": ""
        , "COOPERATIVE": "COOP"
        , "ASSOCIATES": "ASSOC"
        , "MOUNT": "MT"
        , "ADMINISTRATIVE": "ADMIN"
        , "DISTRIBUTORS": "DISTRB"
        , "DISTRIBUTOR": "DISTRB"
        , "HEIGHTS": "HTS"
        , "HEIGHT": "HTS"
        , "DEVELOPMENT": "DEV"
        , "TRANSPORT": "TPT"
        , "HOLDINGS": "HLDGS"
        , "HOLDING": "HLDG"
        , "COUNTY": "CTY"
        , "CONTINENTAL": "CONTL"
        , "AND": "&"
        , "FOUNDATION": "FDN"
        , "CONSTRUCTION": "CONSTR"
        , "EQUIPMENT": "EQ"
        , "AIR CONDITIONING": "AC"
        , "COMMUNITY": "CMTY"
        , "BOARD": "BRD"
        , "EDUCATION": "EDU"
        , "EDUCATIONAL": "EDU"
        , "AUTOMOTIVE": "AUTO"
        , "CENTER": "CTR"
        , "MAINTENANCE": "MNT"
        , "ASSOCIATED": "ASSOC"
        , "AUTHORITY": "AUTH"
        , "ENGLAND": "ENG"
        , "TRANSIT": "TRNST"
        , "MEDICINE": "MED"
        , "UNIVERSITY": "UNI"
        , "PHYS": ""
        , "ADDRE": ""
        , "ATLANTA": "ATL"
        , "HOME CARE": "HOMECARE"
        , "EMPLOYEE": "EMPL"
        , "EMPLOYEES": "EMPL"
        , "INTERNATIONAL": "INTL"
        , "BUILDING": "BLDG"
        , "CONTRACTORS": "CONTRS"
        , "CONTRACTOR": "CONTR"
        , "C O": "CO"
        , "MANUFACTURING": "MFG"
        , "SYSTEM": "SYS"
        , "SYSTEMS": "SYS"
        , "MEDICAL": "MED"
        , "PROGRAM": "PROG"
        , "DIVISION": "DIV"
        , "INVENTORY": "INVEN"
        , "I": ""
        , "II": ""
        , "III": ""
        , "IV": ""
        , "V": ""
        , "COMMUNICATIONS": "COMMS"
        , "INTERNATIONAL": "INTL"
        , "INTERNATIONALINC": "INTL"
        , "PROFESSIONAL": "PROF"
        , "SOLUTIO": "SOLN"
        , "QUALITY": "QLTY"
        , "QUANTITY": "QNTY"
    }

    new_name = name
    # print(new_name)

    # remove punctuations
    for char in punctuation.replace('&', ''):
        new_name = re.sub(r'\s*{}+\s*|\s+'.format(f'\{char}'), ' ', new_name).strip()
    # print(new_name)

    for a in alias:
        new_name = re.sub(r'\bC O\b', 'CO', new_name)

        for word in new_name.split(' '):
            if a == word:
                new_name = re.sub(r'(?=\b{}\b).*'.format(a), '', new_name).strip()
        # print(new_name)

    new_name_l = new_name.split()  # remove dup spaces and words
    new_name_l1 = sorted(list(set(new_name_l)), key=new_name_l.index)
    new_name_1 = ' '.join(new_name_l1)
    # print(new_name_1)

    for k, v in jar.items():  # first filter approx match
        new_name_1 = ' '.join(new_name_1.replace(k, v).split())
        new_name_1 = re.sub(r'\s+', ' ', new_name_1).strip()
    # print(new_name_1)

    if len(new_name_1) <= thres:
        try:
            c0 += 1
        except:
            pass

        return new_name_1
    else:
        for k, v in jar1.items():  # second filter exact match
            new_name_1 = re.sub(r'\b{}\b'.format(k), v, new_name_1).strip()
            new_name_1 = re.sub(r'\s+', ' ', new_name_1).strip()

        for k, v in pot.items():  # abbrev directions
            new_name_1 = re.sub(r'\b{}\b'.format(k), v, new_name_1).strip()
            new_name_1 = re.sub(r'\s+', ' ', new_name_1).strip()

        if len(new_name_1) <= thres:
            try:
                c0 += 1
            except:
                pass

            return new_name_1
        else:
            try:
                c1 += 1
            except:
                pass

            return f'[{new_name_1}]'


def validate_addr(addr1, city, state, zip=''):
    try:
        global d0, d1
    except:
        pass

    # use no after street type for specific zip9
    try:
        try:
            address = Address(
                name='',
                address_1=addr1,
                city=city,
                state=state,
                zipcode=zip
            )

            usps = USPSApi('748MEDLY7441', test=True)
            validation = usps.validate_address(address)
            res = validation.result['AddressValidateResponse']['Address']
            staddr = re.sub(r'\s*-+\s*', ' ', f"{res['Address2']} {res['Address1']}").strip()

            # print(addr1)
        except:
            # remove everything if ambiguous
            suffix = [
                'BLUF',
                'BLUFFS',
                'BLFS',
                'BOTTOM',
                'BOT',
                'BTM',
                'STATE',
                'ALLEY',
                'ALLEE',
                'ALY',
                'ALLY',
                'ANEX',
                'ANX',
                'ANNEX',
                'ANNX',
                'ARCADE',
                'ARC',
                'AVENUE',
                'AV',
                'AVE',
                'AVEN',
                'AVENU',
                'AVN',
                'AVNUE',
                'BAYOU',
                'BAYOO',
                'BYU',
                'BEACH',
                'BCH',
                'BEND',
                'BND',
                'BLUFF',
                'BLF',
                'BOTTM',
                'BOULEVARD',
                'BLVD',
                'BOUL',
                'BOULV',
                'BRANCH',
                'BR',
                'BRNCH',
                'BRIDGE',
                'BRDGE',
                'BRG',
                'BROOK',
                'BRK',
                'BROOKS',
                'BRKS',
                'BURG',
                'BG',
                'BURGS',
                'BGS',
                'BYPASS',
                'BYP',
                'BYPA',
                'BYPAS',
                'BYPS',
                'CAMP',
                'CP',
                'CMP',
                'CANYON ',
                'CANYN',
                'CYN',
                'CANYON',
                'CNYN',
                'CAPE ',
                'CAPE',
                'CPE',
                'CAUSEWAY',
                'CSWY',
                'CAUSWA',
                'CENTER',
                'CEN',
                'CTR',
                'CENT',
                'CENTR',
                'CENTRE',
                'CNTER',
                'CNTR',
                'CENTERS',
                'CTRS',
                'CIRCLE',
                'CIR',
                'CIRC',
                'CIRCL',
                'CRCL',
                'CRCLE',
                'CIRCLES',
                'CIRS',
                'CLIFF',
                'CLF',
                'CLIFFS',
                'CLFS',
                'CLUB',
                'CLB',
                'COMMON',
                'CMN',
                'COMMONS',
                'CMNS',
                'CORNER',
                'COR',
                'CORNERS',
                'CORS',
                'COURSE',
                'CRSE',
                'COURT',
                'CT',
                'COURTS',
                'CTS',
                'COVE',
                'CV',
                'COVES',
                'CVS',
                'CREEK',
                'CRK',
                'CRESCENT',
                'CRES',
                'CRSENT',
                'CRSNT',
                'CREST',
                'CRST',
                'CROSSING',
                'XING',
                'CRSSNG',
                'CROSSROAD',
                'XRD',
                'CROSSROADS',
                'XRDS',
                'CURVE',
                'CURV',
                'DALE',
                'DL',
                'DAM',
                'DM',
                'DIVIDE',
                'DIV',
                'DV',
                'DVD',
                'DRIVE',
                'DR',
                'DRIV',
                'DRV',
                'DRIVES',
                'DRS',
                'ESTATE',
                'EST',
                'ESTATES',
                'ESTS',
                'EXPRESSWAY',
                'EXP',
                'EXPY',
                'EXPR',
                'EXPRESS',
                'EXPW',
                'EXTENSION',
                'EXT',
                'EXTN',
                'EXTNSN',
                'EXTENSIONS',
                'EXTS',
                'FALL',
                'FALLS',
                'FLS',
                'FERRY',
                'FRY',
                'FRRY',
                'FIELD',
                'FLD',
                'FIELDS',
                'FLDS',
                'FLAT',
                'FLT',
                'FLATS',
                'FLTS',
                'FORD',
                'FRD',
                'FORDS',
                'FRDS',
                'FOREST',
                'FRST',
                'FORESTS',
                'FORGE',
                'FORG',
                'FRG',
                'FORGES',
                'FRGS',
                'FORK',
                'FRK',
                'FORKS',
                'FRKS',
                'FORT',
                'FT',
                'FRT',
                'FREEWAY',
                'FWY',
                'FREEWY',
                'FRWAY',
                'FRWY',
                'GARDEN',
                'GDN',
                'GARDN',
                'GRDEN',
                'GRDN',
                'GARDENS',
                'GDNS',
                'GRDNS',
                'GATEWAY',
                'GTWY',
                'GATEWY',
                'GATWAY',
                'GTWAY',
                'GLEN',
                'GLN',
                'GLENS',
                'GLNS',
                'GREEN',
                'GRN',
                'GREENS',
                'GRNS',
                'GROVE',
                'GROV',
                'GRV',
                'GROVES',
                'GRVS',
                'HARBOR',
                'HARB',
                'HBR',
                'HARBR',
                'HRBOR',
                'HARBORS',
                'HBRS',
                'HAVEN',
                'HVN',
                'HEIGHTS',
                'HT',
                'HTS',
                'HIGHWAY',
                'HWY',
                'HIGHWY',
                'HIWAY',
                'HIWY',
                'HWAY',
                'HILL',
                'HL',
                'HILLS',
                'HLS',
                'HOLLOW',
                'HLLW',
                'HOLW',
                'HOLLOWS',
                'HOLWS',
                'INLET',
                'INLT',
                'ISLAND',
                'IS',
                'ISLND',
                'ISLANDS',
                'ISS',
                'ISLNDS',
                'ISLE',
                'ISLES',
                'JUNCTION',
                'JCT',
                'JCTION',
                'JCTN',
                'JUNCTN',
                'JUNCTON',
                'JUNCTIONS',
                'JCTNS',
                'JCTS',
                'KEY',
                'KY',
                'KEYS',
                'KYS',
                'KNOLL',
                'KNL',
                'KNOL',
                'KNOLLS',
                'KNLS',
                'LAKE',
                'LK',
                'LAKES',
                'LKS',
                'LAND',
                'LANDING',
                'LNDG',
                'LNDNG',
                'LANE',
                'LN',
                'LIGHT',
                'LGT',
                'LIGHTS',
                'LGTS',
                'LOAF',
                'LF',
                'LOCK',
                'LCK',
                'LOCKS',
                'LCKS',
                'LODGE',
                'LDG',
                'LDGE',
                'LODG',
                'LOOP',
                'LOOPS',
                'MALL',
                'MANOR',
                'MNR',
                'MANORS',
                'MNRS',
                'MEADOW',
                'MDW',
                'MEADOWS',
                'MDWS',
                'MEDOWS',
                'MEWS',
                'MILL',
                'ML',
                'MILLS',
                'MLS',
                'MISSION',
                'MISSN',
                'MSN',
                'MSSN',
                'MOTORWAY',
                'MTWY',
                'MOUNT',
                'MNT',
                'MT',
                'MOUNTAIN',
                'MNTAIN',
                'MTN',
                'MNTN',
                'MOUNTIN',
                'MTIN',
                'MOUNTAINS',
                'MNTNS',
                'MTNS',
                'NECK',
                'NCK',
                'ORCHARD',
                'ORCH',
                'ORCHRD',
                'OVAL',
                'OVL',
                'OVERPASS',
                'OPAS',
                'PARK',
                'PRK',
                'PARKS',
                'PARKWAY',
                'PKWY',
                'PARKWY',
                'PKWAY',
                'PKY',
                'PARKWAYS',
                'PKWYS',
                'PASS',
                'PASSAGE',
                'PSGE',
                'PATH',
                'PATHS',
                'PIKE',
                'PIKES',
                'PINE',
                'PNE',
                'PINES',
                'PNES',
                'PLACE',
                'PL',
                'PLAIN',
                'PLN',
                'PLAINS',
                'PLNS',
                'PLAZA',
                'PLZ',
                'PLZA',
                'POINT',
                'PT',
                'POINTS',
                'PTS',
                'PORT',
                'PRT',
                'PORTS',
                'PRTS',
                'PRAIRIE',
                'PR',
                'PRR',
                'RADIAL',
                'RAD',
                'RADL',
                'RADIEL',
                'RAMP',
                'RANCH',
                'RNCH',
                'RANCHES',
                'RNCHS',
                'RAPID',
                'RPD',
                'RAPIDS',
                'RPDS',
                'REST',
                'RST',
                'RIDGE',
                'RDG',
                'RDGE',
                'RIDGES',
                'RDGS',
                'RIVER',
                'RIV',
                'RVR',
                'RIVR',
                'ROAD',
                'RD',
                'ROADS',
                'RDS',
                'ROUTE',
                'RTE',
                'ROW',
                'RUE',
                'RUN',
                'SHOAL',
                'SHL',
                'SHOALS',
                'SHLS',
                'SHORE',
                'SHOAR',
                'SHR',
                'SHORES',
                'SHOARS',
                'SHRS',
                'SKYWAY',
                'SKWY',
                'SPRING',
                'SPG',
                'SPNG',
                'SPRNG',
                'SPRINGS',
                'SPGS',
                'SPNGS',
                'SPRNGS',
                'SPUR',
                'SPURS',
                'SQUARE',
                'SQ',
                'SQR',
                'SQRE',
                'SQU',
                'SQUARES',
                'SQRS',
                'SQS',
                'STATION',
                'STA',
                'STATN',
                'STN',
                'STRAVENUE',
                'STRA',
                'STRAV',
                'STRAVEN',
                'STRAVN',
                'STRVN',
                'STRVNUE',
                'STREAM',
                'STRM',
                'STREME',
                'STREET',
                'ST',
                'STRT',
                'STR',
                'STREETS',
                'STS',
                'SUMMIT',
                'SMT',
                'SUMIT',
                'SUMITT',
                'TERRACE',
                'TER',
                'TERR',
                'THROUGHWAY',
                'TRWY',
                'TRACE',
                'TRCE',
                'TRACES',
                'TRACK',
                'TRAK',
                'TRACKS',
                'TRK',
                'TRKS',
                'TRAFFICWAY',
                'TRFY',
                'TRAIL',
                'TRL',
                'TRAILS',
                'TRLS',
                'TRAILER',
                'TRLR',
                'TRLRS',
                'TUNNEL',
                'TUNEL',
                'TUNL',
                'TUNLS',
                'TUNNELS',
                'TUNNL',
                'TURNPIKE',
                'TRNPK',
                'TPKE',
                'TURNPK',
                'UNDERPASS',
                'UPAS',
                'UNION',
                'UN',
                'UNIONS',
                'UNS',
                'VALLEY',
                'VLY',
                'VALLY',
                'VLLY',
                'VALLEYS',
                'VLYS',
                'VIADUCT',
                'VDCT',
                'VIA',
                'VIADCT',
                'VIEW',
                'VW',
                'VIEWS',
                'VWS',
                'VILLAGE',
                'VILL',
                'VLG',
                'VILLAG',
                'VILLG',
                'VILLIAGE',
                'VILLAGES',
                'VLGS',
                'VILLE',
                'VL',
                'VISTA',
                'VIS',
                'VIST',
                'VST',
                'VSTA',
                'WALK',
                'WALKS',
                'WALL',
                'WAY',
                'WY',
                'WAYS',
                'WELL',
                'WL',
                'WELLS',
                'WLS'
            ]

            for typ in suffix:
                for word in addr1.split(' '):
                    if typ == word:
                        addr1 = re.sub(rf'(?<=\b{word}\b).*', '', addr1)

            address = Address(
                name='',
                address_1=addr1,
                city=city,
                state=state,
                zipcode=''
            )

            usps = USPSApi('748MEDLY7441', test=True)
            validation = usps.validate_address(address)
            res = validation.result['AddressValidateResponse']['Address']
            staddr = re.sub(r'\s*-+\s*', ' ', f"{res['Address2']} {res['Address1']}").strip()

            # print(addr1)
        finally:
            fulladdr = f"{staddr},{res['City']},{res['State']} {res['Zip5']}{res['Zip4']}"

            try:
                d0 += 1
            except:
                pass

            return fulladdr
    except:
        fulladdr = ''

        try:
            d1 += 1
        except:
            pass

        return fulladdr


#----
# trim name w dict
#----
os.chdir(r'L:\Billing_Processing\logs')

fw = open('template.txt', 'w')

Ymd = dt.now().strftime('%Y%m%d')

def trim_name_billing():
    with open("""{}.txt""".format(Ymd), 'r') as fh:
        for line in fh:
            if not re.match(r'^\d{7}\s[|]\s', line):
                continue

            if 'changed to' in line:
                continue

            if (len(line)) < (7+3+30):
                continue

            line = line.strip()
            tmp = line.split(' | ')
            # print(tmp)

            pm_sk = tmp[0]
            name = tmp[1]

            changed_name = trim_name(name)
            print("""{} (changed to {})""".format(line, changed_name), file=fw)


#----
# get zip w API
#----
def get_zip_billing():
    try:
        global d0, d1
    except:
        pass

    dicy = {}
    cnt = 0
    with open("""{}.txt""".format(Ymd), 'r') as fh:
        for line in fh:
            if not re.match(r'^I\d{7}', line):
                continue

            if 'to add zip' in line:
                continue

            tmp = [x.strip() for x in line.split('|')]
            pm_sk = tmp[0]
            addr = tmp[1]
            dicy[pm_sk] = addr

            cnt += 1
    # print(dicy)
    # print(cnt)

    for k, v in dicy.items():
        l = v.split(',')

        addr1 = l[0]
        city = l[1]
        state = l[2]
        zip = ''

        fulladdr = validate_addr(addr1, city, state, zip).split(',')

        try:
            zp = fulladdr[2][3:8]

            info = """(to add zip {})""".format(zp)
        except:
            info = ''

        print("""{}|{} {}""".format(k, v, info), file=fw)

    print('\nname changed {}, name remaining {}'.format(str(c0), str(c1)), file=fw)
    print('zip changed {}, zip remaining {}'.format(str(d0), str(d1)), file=fw, end='')

    fw.close()

    with open('template.txt', 'r') as fr:
        print(fr.read())


#----
# transport changes to linux server
#----
def transfer_linux(file, linux_loc):
    try:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.load_system_host_keys()
        ssh.connect(hostname='revproc01.revintel.net',
                    username=user_linux,
                    password=passwd_linux)
    except:
        print('connection ERROR')
    else:
        scp = SCPClient(ssh.get_transport())    # SCPCLient takes paramiko transport as argument
        scp.put(file, linux_loc)

        print('scp transport success...')


if __name__ == '__main__':
    c0, c1, d0, d1 = 0, 0, 0, 0

    trim_name_billing()
    get_zip_billing()
    if not (c0 == d0 == c1 == d1 == 0):
        transfer_linux('template.txt', '/home/james.niu@revintel.net/production/jsondump')
    else:
        print('file not created...')

