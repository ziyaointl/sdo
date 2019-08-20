const puppeteer = require('puppeteer');

(async () => {
    const browser = await puppeteer.launch({headless: true});
    const page = await browser.newPage();
    await page.setViewport({
        width: 1920,
        height: 1080,
        deviceScaleFactor: 1,
      });
    await page.goto('https://portal.nersc.gov/project/desi/users/ziyaoz/history/');
    const links = await page.$$eval('a', (linkHandles) => {
        let lst = [];
        for (let linkHandle of linkHandles) {
            let name = linkHandle.innerHTML;
            if (name.includes('index')) {
                lst.push([name, linkHandle.href]);
            }
        }
        return lst;
    });
    let i = 0;
    for (let pair of links) {
        let [name, url] = pair;
        await page.goto(url);
        await page.screenshot({path: 'screenshots/' + String(i).padStart(4, '0') + '.png'});
        i += 1;
    }
    await browser.close();
})();
