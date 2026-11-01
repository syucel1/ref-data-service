package com.example.refdata.db;

import com.example.refdata.model.Currency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The ISO 4217 currency list (code, numeric code, name, minor units, active flag) written by the generator. */
public final class IsoCurrencies {
    private IsoCurrencies() {
    }

    /** code|numeric|name|minor units|A (active) or W (withdrawn). */
    private static final String[] ROWS = {
        "AED|784|UAE Dirham|2|A", "AFN|971|Afghani|2|A", "ALL|008|Lek|2|A", "AMD|051|Armenian Dram|2|A",
        "ANG|532|Netherlands Antillean Guilder|2|A", "AOA|973|Kwanza|2|A", "ARS|032|Argentine Peso|2|A",
        "AUD|036|Australian Dollar|2|A", "AWG|533|Aruban Florin|2|A", "AZN|944|Azerbaijan Manat|2|A",
        "BAM|977|Convertible Mark|2|A", "BBD|052|Barbados Dollar|2|A", "BDT|050|Taka|2|A", "BGN|975|Bulgarian Lev|2|A",
        "BHD|048|Bahraini Dinar|3|A", "BIF|108|Burundi Franc|0|A", "BMD|060|Bermudian Dollar|2|A", "BND|096|Brunei Dollar|2|A",
        "BOB|068|Boliviano|2|A", "BOV|984|Mvdol|2|A", "BRL|986|Brazilian Real|2|A", "BSD|044|Bahamian Dollar|2|A",
        "BTN|064|Ngultrum|2|A", "BWP|072|Pula|2|A", "BYN|933|Belarusian Ruble|2|A", "BZD|084|Belize Dollar|2|A",
        "CAD|124|Canadian Dollar|2|A", "CDF|976|Congolese Franc|2|A", "CHE|947|WIR Euro|2|A", "CHF|756|Swiss Franc|2|A",
        "CHW|948|WIR Franc|2|A", "CLF|990|Unidad de Fomento|4|A", "CLP|152|Chilean Peso|0|A", "CNY|156|Yuan Renminbi|2|A",
        "COP|170|Colombian Peso|2|A", "COU|970|Unidad de Valor Real|2|A", "CRC|188|Costa Rican Colon|2|A",
        "CUC|931|Peso Convertible|2|A", "CUP|192|Cuban Peso|2|A", "CVE|132|Cabo Verde Escudo|2|A", "CZK|203|Czech Koruna|2|A",
        "DJF|262|Djibouti Franc|0|A", "DKK|208|Danish Krone|2|A", "DOP|214|Dominican Peso|2|A", "DZD|012|Algerian Dinar|2|A",
        "EGP|818|Egyptian Pound|2|A", "ERN|232|Nakfa|2|A", "ETB|230|Ethiopian Birr|2|A", "EUR|978|Euro|2|A",
        "FJD|242|Fiji Dollar|2|A", "FKP|238|Falkland Islands Pound|2|A", "GBP|826|Pound Sterling|2|A", "GEL|981|Lari|2|A",
        "GHS|936|Ghana Cedi|2|A", "GIP|292|Gibraltar Pound|2|A", "GMD|270|Dalasi|2|A", "GNF|324|Guinean Franc|0|A",
        "GTQ|320|Quetzal|2|A", "GYD|328|Guyana Dollar|2|A", "HKD|344|Hong Kong Dollar|2|A", "HNL|340|Lempira|2|A",
        "HTG|332|Gourde|2|A", "HUF|348|Forint|2|A", "IDR|360|Rupiah|2|A", "ILS|376|New Israeli Sheqel|2|A",
        "INR|356|Indian Rupee|2|A", "IQD|368|Iraqi Dinar|3|A", "IRR|364|Iranian Rial|2|A", "ISK|352|Iceland Krona|0|A",
        "JMD|388|Jamaican Dollar|2|A", "JOD|400|Jordanian Dinar|3|A", "JPY|392|Yen|0|A", "KES|404|Kenyan Shilling|2|A",
        "KGS|417|Som|2|A", "KHR|116|Riel|2|A", "KMF|174|Comorian Franc|0|A", "KPW|408|North Korean Won|2|A",
        "KRW|410|Won|0|A", "KWD|414|Kuwaiti Dinar|3|A", "KYD|136|Cayman Islands Dollar|2|A", "KZT|398|Tenge|2|A",
        "LAK|418|Lao Kip|2|A", "LBP|422|Lebanese Pound|2|A", "LKR|144|Sri Lanka Rupee|2|A", "LRD|430|Liberian Dollar|2|A",
        "LSL|426|Loti|2|A", "LYD|434|Libyan Dinar|3|A", "MAD|504|Moroccan Dirham|2|A", "MDL|498|Moldovan Leu|2|A",
        "MGA|969|Malagasy Ariary|2|A", "MKD|807|Denar|2|A", "MMK|104|Kyat|2|A", "MNT|496|Tugrik|2|A", "MOP|446|Pataca|2|A",
        "MRU|929|Ouguiya|2|A", "MUR|480|Mauritius Rupee|2|A", "MVR|462|Rufiyaa|2|A", "MWK|454|Malawi Kwacha|2|A",
        "MXN|484|Mexican Peso|2|A", "MXV|979|Mexican Unidad de Inversion (UDI)|2|A", "MYR|458|Malaysian Ringgit|2|A",
        "MZN|943|Mozambique Metical|2|A", "NAD|516|Namibia Dollar|2|A", "NGN|566|Naira|2|A", "NIO|558|Cordoba Oro|2|A",
        "NOK|578|Norwegian Krone|2|A", "NPR|524|Nepalese Rupee|2|A", "NZD|554|New Zealand Dollar|2|A", "OMR|512|Rial Omani|3|A",
        "PAB|590|Balboa|2|A", "PEN|604|Sol|2|A", "PGK|598|Kina|2|A", "PHP|608|Philippine Peso|2|A", "PKR|586|Pakistan Rupee|2|A",
        "PLN|985|Zloty|2|A", "PYG|600|Guarani|0|A", "QAR|634|Qatari Rial|2|A", "RON|946|Romanian Leu|2|A",
        "RSD|941|Serbian Dinar|2|A", "RUB|643|Russian Ruble|2|A", "RWF|646|Rwanda Franc|0|A", "SAR|682|Saudi Riyal|2|A",
        "SBD|090|Solomon Islands Dollar|2|A", "SCR|690|Seychelles Rupee|2|A", "SDG|938|Sudanese Pound|2|A",
        "SEK|752|Swedish Krona|2|A", "SGD|702|Singapore Dollar|2|A", "SHP|654|Saint Helena Pound|2|A", "SLE|925|Leone|2|A",
        "SOS|706|Somali Shilling|2|A", "SRD|968|Surinam Dollar|2|A", "SSP|728|South Sudanese Pound|2|A",
        "STN|930|Dobra|2|A", "SVC|222|El Salvador Colon|2|A", "SYP|760|Syrian Pound|2|A", "SZL|748|Lilangeni|2|A",
        "THB|764|Baht|2|A", "TJS|972|Somoni|2|A", "TMT|934|Turkmenistan New Manat|2|A", "TND|788|Tunisian Dinar|3|A",
        "TOP|776|Pa'anga|2|A", "TRY|949|Turkish Lira|2|A", "TTD|780|Trinidad and Tobago Dollar|2|A",
        "TWD|901|New Taiwan Dollar|2|A", "TZS|834|Tanzanian Shilling|2|A", "UAH|980|Hryvnia|2|A", "UGX|800|Uganda Shilling|0|A",
        "USD|840|US Dollar|2|A", "USN|997|US Dollar (Next day)|2|A", "UYI|940|Uruguay Peso en Unidades Indexadas|0|A",
        "UYU|858|Peso Uruguayo|2|A", "UYW|927|Unidad Previsional|4|A", "UZS|860|Uzbekistan Sum|2|A",
        "VED|926|Bolivar Soberano|2|A", "VES|928|Bolivar Soberano|2|A", "VND|704|Dong|0|A", "VUV|548|Vatu|0|A",
        "WST|882|Tala|2|A", "XAF|950|CFA Franc BEAC|0|A", "XAG|961|Silver|0|A", "XAU|959|Gold|0|A",
        "XCD|951|East Caribbean Dollar|2|A", "XDR|960|SDR (Special Drawing Right)|0|A", "XOF|952|CFA Franc BCEAO|0|A",
        "XPD|964|Palladium|0|A", "XPF|953|CFP Franc|0|A", "XPT|962|Platinum|0|A", "XSU|994|Sucre|0|A",
        "XUA|965|ADB Unit of Account|0|A", "YER|886|Yemeni Rial|2|A", "ZAR|710|Rand|2|A", "ZMW|967|Zambian Kwacha|2|A",
        "ZWG|924|Zimbabwe Gold|2|A",
        // withdrawn codes still found on historical rows
        "HRK|191|Kuna|2|W", "SLL|694|Leone (old)|2|W", "VEF|937|Bolivar (old)|2|W", "LTL|440|Lithuanian Litas|2|W",
        "LVL|428|Latvian Lats|2|W", "EEK|233|Kroon|2|W", "MRO|478|Ouguiya (old)|2|W", "STD|678|Dobra (old)|2|W",
        "ZWL|932|Zimbabwe Dollar|2|W", "BYR|974|Belarusian Ruble (old)|0|W", "CYP|196|Cyprus Pound|2|W",
        "MTL|470|Maltese Lira|2|W", "SKK|703|Slovak Koruna|2|W", "SIT|705|Tolar|2|W", "DEM|276|Deutsche Mark|2|W",
        "FRF|250|French Franc|2|W", "ITL|380|Italian Lira|0|W", "NLG|528|Netherlands Guilder|2|W", "ESP|724|Spanish Peseta|0|W",
        "BEF|056|Belgian Franc|0|W", "ATS|040|Austrian Schilling|2|W", "FIM|246|Markka|2|W", "IEP|372|Irish Pound|2|W",
        "PTE|620|Portuguese Escudo|0|W", "GRD|300|Drachma|2|W", "LUF|442|Luxembourg Franc|0|W",
    };

    private static final List<Currency> ALL;
    private static final List<Currency> ACTIVE;

    static {
        List<Currency> all = new ArrayList<>(ROWS.length);
        List<Currency> active = new ArrayList<>(ROWS.length);
        for (String row : ROWS) {
            String[] f = row.split("\\|");
            Currency c = new Currency(f[0], Integer.parseInt(f[1]), f[2], Integer.parseInt(f[3]), "A".equals(f[4]));
            all.add(c);
            if (c.active()) active.add(c);
        }
        ALL = Collections.unmodifiableList(all);
        ACTIVE = Collections.unmodifiableList(active);
    }

    /** All currencies, active ones first (in code order), then the withdrawn ones. */
    public static List<Currency> all() {
        return ALL;
    }

    /** Only the currencies that are in use today (accounts and instructions are generated in these). */
    public static List<Currency> active() {
        return ACTIVE;
    }
}
