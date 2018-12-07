// node .\index.js EURUSD 2008-10-01 2008-10-02 -f true
const left_pad = require("leftpad");
const util = require("util");
const lzma = require("lzma-purejs");
const fs = require("fs");
const moment = require("moment");
const download = require("download");
const path = require('path');


const datadir = "./data";
const bidir = datadir + "/bi5";
let prevdata = "";


function mkdir(dir) {
    try {
        fs.mkdirSync(dir);
    }
    catch (e) {
        if (e.errno === 34) {
            mkdir(path.dirname(dir));
            mkdir(dir);
        }
    }
}

function init() {
    if (!fs.existsSync(datadir)) {
        fs.mkdirSync(datadir);
    }
    if (!fs.existsSync(bidir)) {
        fs.mkdirSync(bidir);
    }
}

function dukascopy_url(instrument, date, type) {
    return util.format("http://dukascopy.com/datafeed/%s/%s/%s/%s/" + type + "_candles_min_1.bi5",
        instrument,
        left_pad(date.year(), 4, "0"),
        left_pad(date.month(), 2, "0"),
        left_pad(date.date(), 2, "0"));
}

function processFiles(datadir, bidir, instrument, startdatep, enddate, date, prefixname, filterflat) {
    return new Promise(resolve => {
        let subdir = bidir + "/" + instrument + "/" + left_pad(date.year(), 4, "0") + "/" + left_pad(date.month(), 2, "0");

        let biddata = fs.readFileSync(subdir + "/BID_" + prefixname + ".bi5");
        let askdata = fs.readFileSync(subdir + "/ASK_" + prefixname + ".bi5");

        biddata = lzma.decompressFile(biddata);
        askdata = lzma.decompressFile(askdata);

        biddata = parseCandleBin(biddata, true);
        askdata = parseCandleBin(askdata, true);

        let middata = "";
        for (let i = 0; i <= biddata.length; i++) {
            if (typeof biddata[i] != "undefined" && typeof askdata[i] != "undefined") {
                let abiddata = biddata[i].split(",");
                let aaskdata = askdata[i].split(",");

                let strdate = moment(date).add(parseInt(abiddata[0]), "seconds").format("YYYYMMDDHHmmss"); // Date
                let strvalues = "";

                strvalues += "," + Math.trunc((parseInt(abiddata[1]) + parseInt(aaskdata[1])) / 2); // Open
                strvalues += "," + Math.trunc((parseInt(abiddata[4]) + parseInt(aaskdata[4])) / 2); // High                
                strvalues += "," + Math.trunc((parseInt(abiddata[3]) + parseInt(aaskdata[3])) / 2); // Low
                strvalues += "," + Math.trunc((parseInt(abiddata[2]) + parseInt(aaskdata[2])) / 2); // Close
                strvalues += "," + (parseFloat(abiddata[5]) + parseFloat(aaskdata[5])).toFixed(2); // Volume
                strvalues += "," + Math.abs(parseInt(abiddata[4]) - parseInt(aaskdata[4])); // Spread

                if(filterflat) {
                    if (strvalues != prevdata) {
                        middata += strdate + strvalues + "\r\n";
                    }
                } else {
                    middata += strdate + strvalues + "\r\n";
                }

                prevdata = strvalues;
            }
        }

        fs.writeFileSync(datadir + "/" + instrument + "_" + startdatep.format("YYYYMMDD") + "-" + enddate.format("YYYYMMDD") + ".csv", middata, { flag: "a" });
        resolve("finish");
    });
}

function download1(url, prefixname, bidir) {
    return new Promise(resolve => {
        resolve(download(url, bidir, { 'filename': prefixname + ".bi5" }))
    });
}

async function fetch_date(datadir, startdatep, enddate, instrument, bidir, date, filterflat) {
    let urlbid = dukascopy_url(instrument, date, "BID");
    let urlask = dukascopy_url(instrument, date, "ASK");
    let prefixname = date.format("YYYYMMDD");

    let subdir = bidir + "/" + instrument + "/" + left_pad(date.year(), 4, "0") + "/" + left_pad(date.month(), 2, "0");
    if (!fs.existsSync(subdir)) {
        mkdir(subdir);
    }

    if (fs.existsSync(subdir + "/BID_" + prefixname + ".bi5") && fs.existsSync(subdir + "/ASK_" + prefixname + ".bi5")) {
        await processFiles(datadir, bidir, instrument, startdatep, enddate, date, prefixname, filterflat);
    } else {
        console.log(subdir + "/BID_" + prefixname + ".bi5", " descargo");

        await download1(urlbid, "BID_" + prefixname, subdir);
        await download1(urlask, "ASK_" + prefixname, subdir);

        await processFiles(datadir, bidir, instrument, startdatep, enddate, date, prefixname, filterflat);
    }
}

async function fetch_range(datadir, startdatep, instrument, bidir, start, end, filterflat) {
    // await fetch_date(datadir, startdatep, end, instrument, bidir, start);
    // start.add(1, "days");
    // if(!start.isAfter(end)){
    //     await fetch_range(datadir, startdatep, instrument, bidir, start, end);
    // }
    while (!start.isAfter(end)) {
        await fetch_date(datadir, startdatep, end, instrument, bidir, start, filterflat);
        start.add(1, "days");
    }
}

function parseCandleBin(candledata, inarray) {
    let acandledata = "";
    let fields = 6;
    let fieldbytes = 4;

    if (inarray) {
        acandledata = [];
    }

    let i1, j1, temparray1, chunk1 = fields * fieldbytes;
    for (i1 = 0, j1 = candledata.length; i1 < j1; i1 += chunk1) {
        temparray1 = candledata.slice(i1, i1 + chunk1);

        let i2, j2, temparray2, chunk2 = fieldbytes;
        let cont = 0;
        let tmpcandledata = "";
        for (i2 = 0, j2 = temparray1.length; i2 < j2; i2 += chunk2) {
            temparray2 = temparray1.slice(i2, i2 + chunk2);

            if (cont <= 4) {
                tmpcandledata += temparray2.readInt32BE() + ",";
            } else {
                tmpcandledata += temparray2.readFloatBE();
                if (inarray) {
                    acandledata.push(tmpcandledata);
                } else {
                    acandledata += tmpcandledata + "\r\n";
                }
            }

            cont++;
        }
    }

    return acandledata;
}

module.exports = async (instrument, startdate, enddate, filterflat = true) => {
    const startdatep = moment(startdate);
    let deffile;

    init();
    if (typeof enddate == "undefined") {
        enddate = moment.utc();
    } else {
        enddate = moment.utc(enddate);
    }

    if (typeof instrument != "undefined" && typeof startdate != "undefined") {
        startdate = moment.utc(startdate);
        deffile = "./data/" + instrument + "_" + startdate.format("YYYYMMDD") + "-" + enddate.format("YYYYMMDD") + ".csv";
        if (fs.existsSync(deffile)) {
            fs.unlinkSync(deffile);
        }
        await fetch_range(datadir, startdatep, instrument, bidir, startdate, enddate, filterflat);
    }

    return deffile;
}
