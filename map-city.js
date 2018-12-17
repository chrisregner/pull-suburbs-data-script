const { get } = require('axios')
const oboe = require('oboe')
const fs = require('fs')

console.time('pull-suburbs-data')

/**
 * Constants
 */
const { GEOCODE_API, GEOCODE_KEY, INPUT_DIR, OUTPUT_DIR } = require('./const')
const MAIN_CITIES = ['Sydney', 'Perth', 'Melbourne', 'Brisbane']
const REG_TO_CITY = {
  'New South Wales': 'Sydney',
  'Northern Territory': 'Darwin',
  'Western Australia': 'Perth',
  'Victoria': 'Melbourne',
  'Queensland': 'Brisbane',
  'Tasmania': 'Hobart',
  'Australian Capital Territory': 'Canberra',
  'South Australia': 'Adelaide',
}


/**
 * Create output directory if non-existent
 */
if (!fs.existsSync(OUTPUT_DIR))
  fs.mkdirSync(OUTPUT_DIR)


/**
 * Main logic
 */

let output = []
let entries = 0
const input$ = fs.createReadStream(`${INPUT_DIR}/input-map.json`)
const timestamp = +new Date()

oboe(input$)
  .node('{lat lng city suburb}', ({ suburb, city, lat, lng }) => {
    // No idea why `REG_TO_CITY[city]` evaluates to `undefined`
    const mappedEntry = Object
      .entries(REG_TO_CITY)
      .find(([k]) => {
        console.log(String(k), String(city))

        return k === city
      })
    const mappedCity = mappedEntry && mappedEntry[1]

    if (MAIN_CITIES.includes(mappedCity)) {
      entries++
      output.push({ lat, lng, suburb, city: mappedCity })
    }
  })
  .done(() => {
    const output$ = fs.createWriteStream(`${OUTPUT_DIR}/output-${timestamp}.json`)
    output$.write(JSON.stringify(output, null, 2))
    output$.end()
    console.log(`Created ${entries} entries`)
  })
