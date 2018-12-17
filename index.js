const { get } = require('axios')
const oboe = require('oboe')
const fs = require('fs')

console.time('pull-suburbs-data')

/**
 * Constants
 */
const GEOCODE_API = 'https://maps.googleapis.com/maps/api/geocode/json'
const GEOCODE_KEY = process.env.API_KEY
const INPUT_DIR = 'input'
const OUTPUT_DIR = 'output'

const REQUEST_COUNT_PER_BATCH = 200
const REQUEST_BATCH_INTERVAL = 1000

const ENOTFOUND_ERR = 'getaddrinfo ENOTFOUND maps.googleapis.com maps.googleapis.com:443'
const SOCKET_HANGUP_ERR = 'socket hang up'
const NO_LATLNG_MATCH_ERR = 'LatLng matched no address component'
const NO_CITYSUBURB_MATCH_ERR = 'City/Suburb rule matched no address component'


/**
 * Create output directory if non-existent
 */
if (!fs.existsSync(OUTPUT_DIR))
  fs.mkdirSync(OUTPUT_DIR)

/**
 * Main logic
 */
const timestamp = +new Date()
const requestFns = []
const entries = []

let errors$
let output = []
const input$ = fs.createReadStream(`${INPUT_DIR}/input.json`)

console.log('Reading input file...')

oboe(input$)
  /** For each entry in the input json, let's create a thunk and push it to `requestFns` */
  .node('{id lat lng}', ({ id, lat, lng }) => {
    requestFns.push(
      () => requestCityAndSuburb({ id, lat, lng })
        .then(({ suburb, city }) => {
          /** Deduplicate by city/suburb */
          const entryId = `${city}/${suburb}`

          if (entries.includes(entryId))
            return

          entries.push(entryId)

          /** Push data */
          output.push({ lat, lng, suburb, city })
        })
        .catch(console.error)
    )
  })
  /** Batch-requests of requestFns */
  .done(() => {
    let batchNo = 1
    const totalBatchCount = Math.ceil(requestFns.length / REQUEST_COUNT_PER_BATCH)

    console.log('Requesting data from Google Geocoding API...')

    const requestByBatchLoop = () => {
      console.log(`Requesting batch ${batchNo} of ${totalBatchCount}...`)

      batchNo++

      /* Slice a batch */
      const batchedRequestFns = requestFns.splice(0, REQUEST_COUNT_PER_BATCH)

      /* Call/unwrap batch of thunks */
      const batchedRequestsP = batchedRequestFns.map(requestFn => requestFn())

      Promise.all(batchedRequestsP)
        .then(() => {
          /** Continue recursively as long as there's thunks */
          if (requestFns.length) {
            setTimeout(requestByBatchLoop, REQUEST_BATCH_INTERVAL)
            return
          }

          /** Write output when finished */
          console.log(`Requests finished!`)

          const output$ = fs.createWriteStream(`${OUTPUT_DIR}/output-${timestamp}.json`)
          output$.write(JSON.stringify(output, null, 2))
          output$.end()

          if (errors$)
            errors$.end()

          console.timeEnd('pull-suburbs-data')
          console.log(`Created ${entries.length} entries`)
        })
    }

    requestByBatchLoop()
  })

const requestCityAndSuburb = ({ id, lat, lng }) =>
  get(GEOCODE_API, {
    params: {
      key: GEOCODE_KEY,
      latlng: `${lat},${lng}`,
    }
  })
    .then(({ data: { results } }) => {
      let citySuburb

      if (!results.length)
        throw new Error(NO_LATLNG_MATCH_ERR)

      for (let { address_components } of results) {
        const suburbObj = address_components
          .find(ac => ac.types.includes('locality'))
        const suburb = suburbObj && suburbObj.long_name

        const cityObj = address_components
          .find(ac => ac.types.includes('administrative_area_level_1'))
        const city = cityObj && cityObj.long_name

        if (suburb && city) {
          citySuburb = { city, suburb }
          break
        }
      }

      /** Log when no city/suburb is matched */
      if (!citySuburb) {
        if (!errors$)
          errors$ = fs.createWriteStream(`${OUTPUT_DIR}/errors-${timestamp}.txt`)

        errors$.write(`${id}: ${JSON.stringify(results, null, 2)}\n`)
        throw new Error(NO_CITYSUBURB_MATCH_ERR)
      }

      return citySuburb
    })
    .catch(error => {
      /**
       * These are non-deterministic errors (i.e. same call doesn't always end up with same result)
       * so let's just keep on re-requesting when they occur
       */
      const RECON_ERRS = [ENOTFOUND_ERR, NO_LATLNG_MATCH_ERR, SOCKET_HANGUP_ERR]

      if (RECON_ERRS.includes(trimErrMsg(String(error))))
        return requestCityAndSuburb({ id, lat, lng })

      if (!errors$)
        errors$ = fs.createWriteStream(`${OUTPUT_DIR}/errors-${timestamp}.txt`)

      errors$.write(`${id}: ${error}\n`)
    })

const trimErrMsg = s => s.replace('Error: ', '')
