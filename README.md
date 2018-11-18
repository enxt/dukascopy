Dukascopy 1 minute downloader

how to use:
```js
const dukas = require("dukascopy");

async function descarga() {
    await dukas("EURUSD", "2017-01-01", "2017-01-10");;
    console.log("downloaded");
}

descarga();
```
