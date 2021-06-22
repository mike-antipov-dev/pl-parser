exports.handler = async (event) => {
    const _ = require('lodash'),
        Downloader = require('nodejs-file-downloader'),
        mysql = require('mysql2/promise'),
        fs = require('fs').promises,
        neatCsv = require('neat-csv'),
        iconv = require('iconv-lite'),
        connection = await mysql.createConnection({
            host: process.env.HOST,
            user: process.env.USER,
            database: process.env.DB,
            password: process.env.PW
        });
    let csvData = [],
        values = '',
        assembledNumber = '';

    // Download and parse all files into the final query variable
    return Promise.all([
        downloadFile('https://numeracja.uke.gov.pl/en/pstn_tables/export.csv', 'pstn')
            .then(async () => {
                await parseFile('/tmp/pstn.csv')
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/plmn_tables/export.csv', 'plmn')
            .then(async () => {
                await parseFile('/tmp/plmn.csv');
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/mnc_tables/export.csv', 'mnc')
            .then(async () => {
                await parseFile('/tmp/mnc.csv');
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/ndsi_tables/export.csv', 'ndsi')
            .then(async () => {
                await parseFile('/tmp/ndsi.csv');
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/aus_tables/export.csv', 'aus')
            .then(async () => {
                await parseFile('/tmp/aus.csv');
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/hesc_tables/export.csv', 'hesc')
            .then(async () => {
                await parseFile('/tmp/hesc.csv');
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/ndin_tables/export.csv', 'ndin')
            .then(async () => {
                await parseFile('/tmp/ndin.csv');
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/nds_tables/export.csv', 'nds')
            .then(async () => {
                await parseFile('/tmp/nds.csv');
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/ispc_tables/export.csv', 'ispc')
            .then(async () => {
                await parseFile('/tmp/ispc.csv');
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/nspc_tables/export.csv', 'nspc')
            .then(async () => {
                await parseFile('/tmp/nspc.csv');
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/voip_tables/export.csv', 'voip')
            .then(async () => {
                await parseFile('/tmp/voip.csv');
            }),
        downloadFile('https://numeracja.uke.gov.pl/en/nrnp_tables/export.csv', 'nrnp')
            .then(async () => {
                await parseFile('/tmp/nrnp.csv');
            })
    ])
        .then(async () => {
            csvData =  csvData.flat(1);
            await processArray(csvData);
        })
        .then(async () => {
            await connection.query('TRUNCATE TABLE pl_numbers');
            values = prepareValues(values);
        })
        .then(async () => {
            try {
                await connection.query(`INSERT INTO pl_numbers (number, provider_id, provider, mod_date, zone_name, zone_symbol, zone_ab, num_area, number_type, service_description, entity_services, service_type_name, routing_type, area_of_use, network, location) VALUES ${values}`);
            } catch (e) {
                console.error('caught exception!', e);
            }
        })
        .finally(() => {
            connection.end;
            console.log('Done');
        });

    // Pass every CSV row to the number parser
    async function processArray(array) {
        array.map(async (element) => {
            // Regex for checking if a 'Zakres (number)' is actually a single number
            let numberToCheck = element['Zakres (number)'],
                regex = /\(/g,
                parenthesis;
            parenthesis = typeof(numberToCheck) != 'undefined' ? numberToCheck.match(regex) : null;
            if (element.hasOwnProperty('Zakres (number)') && parenthesis != null && parenthesis.length == 1) {
                // If a number contains range, pass it to the parseNums function
                await parseNums(element['Zakres (number)'], element);
            } else if (element.hasOwnProperty('Zakres (number)') && parenthesis == null) {
                // Parse single number CSVs with 'Zakres (number)'
                values += `(${element['Zakres (number)']}, ${element['Operator (ID)']}, '${element['Operator (nazwa)']}', '${element['Data modyfikacji']}', '${element['Strefa (nazwa)']}', '${element['Strefa (symbol)']}', '${element['Strefa (AB)']}', '${element['Obszar numeracyjny']}', '${element['Rodzaj numeru]']}', '${element['Opis uslugi']}', '${element['Podmiot świadczący usługi']}', '${element['Nazwa typu usługi']}', '${element['Typ numeru rutingowego']}', '${element['Obszar']}', '${element['Sieć']}', '${element['Lokalizacja']}'), `;
            } else {
                // Remove dashes and spaces from numbers for some CSVs
                if (typeof(element['Numer']) != 'undefined') {
                    element['Numer'] = element['Numer'].replace(/-| |,/g, '');
                }
                // Parse single number CSVs with 'Numer'
                values += `(${element['Numer']}, ${element['Operator (ID)']}, '${element['Operator (nazwa)']}', '${element['Data modyfikacji']}', '${element['Strefa (nazwa)']}', '${element['Strefa (symbol)']}', '${element['Strefa (AB)']}', '${element['Obszar numeracyjny']}', '${element['Rodzaj numeru]']}', '${element['Opis uslugi']}', '${element['Podmiot świadczący usługi']}', '${element['Nazwa typu usługi']}', '${element['Typ numeru rutingowego']}', '${element['Obszar']}', '${element['Sieć']}', '${element['Lokalizacja']}'), `;
            }
        });
    }

    // Download files
    async function downloadFile(link, name) {
        await new Downloader({
            url: link,
            directory: '/tmp',
            cloneFiles: false,
            fileName: name + '.csv'
            }).download();
        console.log(name + ' has been downloaded.')
    }

    // Parse CSVs
    async function parseFile(link) {
        let file = await fs.readFile(link);
        file = iconv.decode(file, 'win1250');
        let result = await neatCsv(file, { separator: ';' });
        csvData.push(result);
        console.log(link + ' has been parsed.');
    }

    // Remove parenthesis and commas from a number
    function sanitize(input, array) {
        for (let element of input) {
            regex = /(?:\(|\)|,)/g,
            element = element[0].replace(regex, '');
            if(!element == '') {
                array.push(parseInt(element));
            }
        }
    }

    // Replace 'undefined' with NULL and remove trailing comma
    function prepareValues(values) {
        values = values.replace(/undefined/g, 'NULL');
        values = values.slice(0, values.length - 2);
        return values;
    }

    // Convert ranges to an array with numbers
    async function parseNums (nums, object) {
        let ranges = nums.matchAll(/\d-\d/g),
            rangeArr = [];
        // Extract range values as array per range
        for (const element of ranges) {
            let tempArr = element[0].split('-');
            // Add separate nums within range
            rangeArr = (_.range(tempArr[0], tempArr[1]));
            // Add missing last number of range
            rangeArr.push(parseInt(tempArr[1]));
        }
        // Find separate numbers
        let leftParenthesis = nums.matchAll(/\(\d,/g),
            rightParenthesis = nums.matchAll(/,\d\)/g),
            betweenCommas = nums.matchAll(/(?<![^,])\d(?![^,])/g),
            firstPart = nums.matchAll(/[0-9]*/g);
        
        leftParenthesis = sanitize(leftParenthesis, rangeArr),
        rightParenthesis = sanitize(rightParenthesis, rangeArr),
        betweenCommas = sanitize(betweenCommas, rangeArr);
        firstPart = [...firstPart];
        firstPart = firstPart[0][0];
        // Assemble a number and insert it into the values variable
        rangeArr.map((element) => {
            assembledNumber = firstPart + element;
            object['Zakres (number)'] = assembledNumber;
            values += `(${object['Zakres (number)']}, ${object['Operator (ID)']}, '${object['Operator (nazwa)']}', '${object['Data modyfikacji']}', '${object['Strefa (nazwa)']}', '${object['Strefa (symbol)']}', '${object['Strefa (AB)']}', '${object['Obszar numeracyjny']}', '${object['Rodzaj numeru]']}', '${object['Opis uslugi']}', '${object['Podmiot świadczący usługi']}', '${object['Nazwa typu usługi']}', '${object['Typ numeru rutingowego']}', '${object['Obszar']}', '${object['Sieć']}', '${object['Lokalizacja']}'), `;
        });
    }
}