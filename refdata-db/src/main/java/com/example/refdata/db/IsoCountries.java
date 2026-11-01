package com.example.refdata.db;

import com.example.refdata.model.Country;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The ISO 3166-1 country list (alpha-2, alpha-3, numeric code, short name, UN region) written by the generator. */
public final class IsoCountries {
    private IsoCountries() {
    }

    private static final String[] ROWS = {
        "AD|AND|020|Andorra|Europe", "AE|ARE|784|United Arab Emirates|Asia", "AF|AFG|004|Afghanistan|Asia",
        "AG|ATG|028|Antigua and Barbuda|Americas", "AI|AIA|660|Anguilla|Americas", "AL|ALB|008|Albania|Europe",
        "AM|ARM|051|Armenia|Asia", "AO|AGO|024|Angola|Africa", "AQ|ATA|010|Antarctica|Antarctica",
        "AR|ARG|032|Argentina|Americas", "AS|ASM|016|American Samoa|Oceania", "AT|AUT|040|Austria|Europe",
        "AU|AUS|036|Australia|Oceania", "AW|ABW|533|Aruba|Americas", "AX|ALA|248|Aland Islands|Europe",
        "AZ|AZE|031|Azerbaijan|Asia", "BA|BIH|070|Bosnia and Herzegovina|Europe", "BB|BRB|052|Barbados|Americas",
        "BD|BGD|050|Bangladesh|Asia", "BE|BEL|056|Belgium|Europe", "BF|BFA|854|Burkina Faso|Africa",
        "BG|BGR|100|Bulgaria|Europe", "BH|BHR|048|Bahrain|Asia", "BI|BDI|108|Burundi|Africa", "BJ|BEN|204|Benin|Africa",
        "BL|BLM|652|Saint Barthelemy|Americas", "BM|BMU|060|Bermuda|Americas", "BN|BRN|096|Brunei Darussalam|Asia",
        "BO|BOL|068|Bolivia|Americas", "BQ|BES|535|Bonaire, Sint Eustatius and Saba|Americas", "BR|BRA|076|Brazil|Americas",
        "BS|BHS|044|Bahamas|Americas", "BT|BTN|064|Bhutan|Asia", "BV|BVT|074|Bouvet Island|Americas",
        "BW|BWA|072|Botswana|Africa", "BY|BLR|112|Belarus|Europe", "BZ|BLZ|084|Belize|Americas", "CA|CAN|124|Canada|Americas",
        "CC|CCK|166|Cocos (Keeling) Islands|Oceania", "CD|COD|180|Congo, Democratic Republic of the|Africa",
        "CF|CAF|140|Central African Republic|Africa", "CG|COG|178|Congo|Africa", "CH|CHE|756|Switzerland|Europe",
        "CI|CIV|384|Cote d'Ivoire|Africa", "CK|COK|184|Cook Islands|Oceania", "CL|CHL|152|Chile|Americas",
        "CM|CMR|120|Cameroon|Africa", "CN|CHN|156|China|Asia", "CO|COL|170|Colombia|Americas", "CR|CRI|188|Costa Rica|Americas",
        "CU|CUB|192|Cuba|Americas", "CV|CPV|132|Cabo Verde|Africa", "CW|CUW|531|Curacao|Americas",
        "CX|CXR|162|Christmas Island|Oceania", "CY|CYP|196|Cyprus|Asia", "CZ|CZE|203|Czechia|Europe", "DE|DEU|276|Germany|Europe",
        "DJ|DJI|262|Djibouti|Africa", "DK|DNK|208|Denmark|Europe", "DM|DMA|212|Dominica|Americas",
        "DO|DOM|214|Dominican Republic|Americas", "DZ|DZA|012|Algeria|Africa", "EC|ECU|218|Ecuador|Americas",
        "EE|EST|233|Estonia|Europe", "EG|EGY|818|Egypt|Africa", "EH|ESH|732|Western Sahara|Africa", "ER|ERI|232|Eritrea|Africa",
        "ES|ESP|724|Spain|Europe", "ET|ETH|231|Ethiopia|Africa", "FI|FIN|246|Finland|Europe", "FJ|FJI|242|Fiji|Oceania",
        "FK|FLK|238|Falkland Islands (Malvinas)|Americas", "FM|FSM|583|Micronesia, Federated States of|Oceania",
        "FO|FRO|234|Faroe Islands|Europe", "FR|FRA|250|France|Europe", "GA|GAB|266|Gabon|Africa",
        "GB|GBR|826|United Kingdom|Europe", "GD|GRD|308|Grenada|Americas", "GE|GEO|268|Georgia|Asia",
        "GF|GUF|254|French Guiana|Americas", "GG|GGY|831|Guernsey|Europe", "GH|GHA|288|Ghana|Africa", "GI|GIB|292|Gibraltar|Europe",
        "GL|GRL|304|Greenland|Americas", "GM|GMB|270|Gambia|Africa", "GN|GIN|324|Guinea|Africa", "GP|GLP|312|Guadeloupe|Americas",
        "GQ|GNQ|226|Equatorial Guinea|Africa", "GR|GRC|300|Greece|Europe",
        "GS|SGS|239|South Georgia and the South Sandwich Islands|Americas", "GT|GTM|320|Guatemala|Americas",
        "GU|GUM|316|Guam|Oceania", "GW|GNB|624|Guinea-Bissau|Africa", "GY|GUY|328|Guyana|Americas", "HK|HKG|344|Hong Kong|Asia",
        "HM|HMD|334|Heard Island and McDonald Islands|Oceania", "HN|HND|340|Honduras|Americas", "HR|HRV|191|Croatia|Europe",
        "HT|HTI|332|Haiti|Americas", "HU|HUN|348|Hungary|Europe", "ID|IDN|360|Indonesia|Asia", "IE|IRL|372|Ireland|Europe",
        "IL|ISR|376|Israel|Asia", "IM|IMN|833|Isle of Man|Europe", "IN|IND|356|India|Asia",
        "IO|IOT|086|British Indian Ocean Territory|Africa", "IQ|IRQ|368|Iraq|Asia", "IR|IRN|364|Iran, Islamic Republic of|Asia",
        "IS|ISL|352|Iceland|Europe", "IT|ITA|380|Italy|Europe", "JE|JEY|832|Jersey|Europe", "JM|JAM|388|Jamaica|Americas",
        "JO|JOR|400|Jordan|Asia", "JP|JPN|392|Japan|Asia", "KE|KEN|404|Kenya|Africa", "KG|KGZ|417|Kyrgyzstan|Asia",
        "KH|KHM|116|Cambodia|Asia", "KI|KIR|296|Kiribati|Oceania", "KM|COM|174|Comoros|Africa",
        "KN|KNA|659|Saint Kitts and Nevis|Americas", "KP|PRK|408|Korea, Democratic People's Republic of|Asia",
        "KR|KOR|410|Korea, Republic of|Asia", "KW|KWT|414|Kuwait|Asia", "KY|CYM|136|Cayman Islands|Americas",
        "KZ|KAZ|398|Kazakhstan|Asia", "LA|LAO|418|Lao People's Democratic Republic|Asia", "LB|LBN|422|Lebanon|Asia",
        "LC|LCA|662|Saint Lucia|Americas", "LI|LIE|438|Liechtenstein|Europe", "LK|LKA|144|Sri Lanka|Asia",
        "LR|LBR|430|Liberia|Africa", "LS|LSO|426|Lesotho|Africa", "LT|LTU|440|Lithuania|Europe", "LU|LUX|442|Luxembourg|Europe",
        "LV|LVA|428|Latvia|Europe", "LY|LBY|434|Libya|Africa", "MA|MAR|504|Morocco|Africa", "MC|MCO|492|Monaco|Europe",
        "MD|MDA|498|Moldova, Republic of|Europe", "ME|MNE|499|Montenegro|Europe", "MF|MAF|663|Saint Martin (French part)|Americas",
        "MG|MDG|450|Madagascar|Africa", "MH|MHL|584|Marshall Islands|Oceania", "MK|MKD|807|North Macedonia|Europe",
        "ML|MLI|466|Mali|Africa", "MM|MMR|104|Myanmar|Asia", "MN|MNG|496|Mongolia|Asia", "MO|MAC|446|Macao|Asia",
        "MP|MNP|580|Northern Mariana Islands|Oceania", "MQ|MTQ|474|Martinique|Americas", "MR|MRT|478|Mauritania|Africa",
        "MS|MSR|500|Montserrat|Americas", "MT|MLT|470|Malta|Europe", "MU|MUS|480|Mauritius|Africa", "MV|MDV|462|Maldives|Asia",
        "MW|MWI|454|Malawi|Africa", "MX|MEX|484|Mexico|Americas", "MY|MYS|458|Malaysia|Asia", "MZ|MOZ|508|Mozambique|Africa",
        "NA|NAM|516|Namibia|Africa", "NC|NCL|540|New Caledonia|Oceania", "NE|NER|562|Niger|Africa",
        "NF|NFK|574|Norfolk Island|Oceania", "NG|NGA|566|Nigeria|Africa", "NI|NIC|558|Nicaragua|Americas",
        "NL|NLD|528|Netherlands|Europe", "NO|NOR|578|Norway|Europe", "NP|NPL|524|Nepal|Asia", "NR|NRU|520|Nauru|Oceania",
        "NU|NIU|570|Niue|Oceania", "NZ|NZL|554|New Zealand|Oceania", "OM|OMN|512|Oman|Asia", "PA|PAN|591|Panama|Americas",
        "PE|PER|604|Peru|Americas", "PF|PYF|258|French Polynesia|Oceania", "PG|PNG|598|Papua New Guinea|Oceania",
        "PH|PHL|608|Philippines|Asia", "PK|PAK|586|Pakistan|Asia", "PL|POL|616|Poland|Europe",
        "PM|SPM|666|Saint Pierre and Miquelon|Americas", "PN|PCN|612|Pitcairn|Oceania", "PR|PRI|630|Puerto Rico|Americas",
        "PS|PSE|275|Palestine, State of|Asia", "PT|PRT|620|Portugal|Europe", "PW|PLW|585|Palau|Oceania",
        "PY|PRY|600|Paraguay|Americas", "QA|QAT|634|Qatar|Asia", "RE|REU|638|Reunion|Africa", "RO|ROU|642|Romania|Europe",
        "RS|SRB|688|Serbia|Europe", "RU|RUS|643|Russian Federation|Europe", "RW|RWA|646|Rwanda|Africa",
        "SA|SAU|682|Saudi Arabia|Asia", "SB|SLB|090|Solomon Islands|Oceania", "SC|SYC|690|Seychelles|Africa",
        "SD|SDN|729|Sudan|Africa", "SE|SWE|752|Sweden|Europe", "SG|SGP|702|Singapore|Asia",
        "SH|SHN|654|Saint Helena, Ascension and Tristan da Cunha|Africa", "SI|SVN|705|Slovenia|Europe",
        "SJ|SJM|744|Svalbard and Jan Mayen|Europe", "SK|SVK|703|Slovakia|Europe", "SL|SLE|694|Sierra Leone|Africa",
        "SM|SMR|674|San Marino|Europe", "SN|SEN|686|Senegal|Africa", "SO|SOM|706|Somalia|Africa", "SR|SUR|740|Suriname|Americas",
        "SS|SSD|728|South Sudan|Africa", "ST|STP|678|Sao Tome and Principe|Africa", "SV|SLV|222|El Salvador|Americas",
        "SX|SXM|534|Sint Maarten (Dutch part)|Americas", "SY|SYR|760|Syrian Arab Republic|Asia", "SZ|SWZ|748|Eswatini|Africa",
        "TC|TCA|796|Turks and Caicos Islands|Americas", "TD|TCD|148|Chad|Africa",
        "TF|ATF|260|French Southern Territories|Africa", "TG|TGO|768|Togo|Africa", "TH|THA|764|Thailand|Asia",
        "TJ|TJK|762|Tajikistan|Asia", "TK|TKL|772|Tokelau|Oceania", "TL|TLS|626|Timor-Leste|Asia", "TM|TKM|795|Turkmenistan|Asia",
        "TN|TUN|788|Tunisia|Africa", "TO|TON|776|Tonga|Oceania", "TR|TUR|792|Turkiye|Asia", "TT|TTO|780|Trinidad and Tobago|Americas",
        "TV|TUV|798|Tuvalu|Oceania", "TW|TWN|158|Taiwan, Province of China|Asia", "TZ|TZA|834|Tanzania, United Republic of|Africa",
        "UA|UKR|804|Ukraine|Europe", "UG|UGA|800|Uganda|Africa", "UM|UMI|581|United States Minor Outlying Islands|Oceania",
        "US|USA|840|United States of America|Americas", "UY|URY|858|Uruguay|Americas", "UZ|UZB|860|Uzbekistan|Asia",
        "VA|VAT|336|Holy See|Europe", "VC|VCT|670|Saint Vincent and the Grenadines|Americas",
        "VE|VEN|862|Venezuela, Bolivarian Republic of|Americas", "VG|VGB|092|Virgin Islands, British|Americas",
        "VI|VIR|850|Virgin Islands, U.S.|Americas", "VN|VNM|704|Viet Nam|Asia", "VU|VUT|548|Vanuatu|Oceania",
        "WF|WLF|876|Wallis and Futuna|Oceania", "WS|WSM|882|Samoa|Oceania", "YE|YEM|887|Yemen|Asia", "YT|MYT|175|Mayotte|Africa",
        "ZA|ZAF|710|South Africa|Africa", "ZM|ZMB|894|Zambia|Africa", "ZW|ZWE|716|Zimbabwe|Africa",
    };

    private static final List<Country> ALL;

    static {
        List<Country> list = new ArrayList<>(ROWS.length);
        for (String row : ROWS) {
            String[] f = row.split("\\|");
            list.add(new Country(f[0], f[1], Integer.parseInt(f[2]), f[3], f[4]));
        }
        ALL = Collections.unmodifiableList(list);
    }

    /** All countries, sorted by alpha-2 code. */
    public static List<Country> all() {
        return ALL;
    }
}
