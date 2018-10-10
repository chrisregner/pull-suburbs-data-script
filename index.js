const { get } = require('axios')
const oboe = require('oboe')
const fs = require('fs')

const {
  flatten,
  path,
  map,
  find,
  contains,
  prop,
  pipe,
  allPass,
  objOf,
} = require('ramda')

console.time('fill-up-suburbs-data')

const GEOCODE_API = 'https://maps.googleapis.com/maps/api/geocode/json'
const GEOCODE_KEY = process.env.API_KEY
const INPUT_DIR = 'input'
const OUTPUT_DIR = 'output'

const REQUEST_COUNT_PER_BATCH = 200
const REQUEST_BATCH_INTERVAL = 1000

const ENOTFOUND_ERR = 'getaddrinfo ENOTFOUND maps.googleapis.com maps.googleapis.com:443'
const SOCKET_HANGUP_ERR = 'socket hang up'
const NO_LATLNG_MATCH_ERR = 'LatLng matched no address component'
const NO_SUBURB_MATCH_ERR = 'Suburb rule matched no address component'

if (!fs.existsSync(OUTPUT_DIR))
  fs.mkdirSync(OUTPUT_DIR)

const timestamp = +new Date()
const requestsFn = []
const venuesCountBySuburb = {}
let isStart = true

let errors$
let output = []
const input$ = fs.createReadStream(`${INPUT_DIR}/input.json`)
const output$ = fs.createWriteStream(`${OUTPUT_DIR}/output-${timestamp}.json`)

output.push('[')

console.log('Reading input file...')

oboe(input$)
  .node('{id lat lng}', ({ id, lat, lng, timezone }) => {
    requestsFn.push(
      () => requestSuburb({ id, lat, lng })
        .then(({ suburb, error }) => {
          const city = timezoneToCity(timezone)
          const entryId = `${city}/${suburb}`

          if (venuesCountBySuburb[entryId]) {
            venuesCountBySuburb[entryId]++
            return
          }

          venuesCountBySuburb[entryId] = 1

          const dataKey = error ? 'error' : 'suburb'

          // Add comma on previous object as necessary
          if (isStart){
            isStart = false
          } else {
            const lastIndex = output.length - 1
            output[lastIndex] = output[lastIndex] + ','
          }

          output = output.concat([
            '  {',
            `    "lat": ${lat},`,
            `    "lng": ${lng},`,
            `    "city": "${city}",`,
            `    "${dataKey}": "${error || suburb}",`, // keep comma, venues_count will go after
            '  }'
          ])
        })
        .catch(console.error)
    )
  })
  .done(() => {
    let batchNo = 1
    const totalBatchCount = Math.ceil(requestsFn.length / REQUEST_COUNT_PER_BATCH)
    console.log('Requesting data from Google Geocoding API...')

    const requestByBatchLoop = () => {
      console.log(`Requesting batch ${batchNo} of ${totalBatchCount}...`)
      batchNo++

      const batchedRequestsFn = requestsFn.splice(0, REQUEST_COUNT_PER_BATCH)
      const batchedRequestsP = batchedRequestsFn.map(requestFn => requestFn())

      Promise.all(batchedRequestsP)
        .then(() => {
          if (requestsFn.length) {
            setTimeout(requestByBatchLoop, REQUEST_BATCH_INTERVAL)
            return
          }

          console.log(`Requests finished!`)

          output.push(']')

          Object.entries(venuesCountBySuburb).forEach(([id, count]) => {
            const [city, suburb] = id.split('/')
            const cityIndex = output.findIndex((cityLine, i) => {
              const suburbLine = output[i + 1]
              const isSuburbCorrect = suburbLine.includes(`"suburb": "${suburb}"`)
              const isCityCorrect = isSuburbCorrect && cityLine.includes(`"city": "${city}"`)

              return isSuburbCorrect && isCityCorrect
            })

            output.splice(cityIndex + 2, 0, `    "venues_count": "${count}"`);
          })

          output = output.join('\n')
          output$.write(output)
          output$.end()

          if (errors$)
            errors$.end()

          const entries = Object.keys(venuesCountBySuburb).length

          console.timeEnd('fill-up-suburbs-data')
          console.log(`Created ${entries} entries`)
        })
    }

    requestByBatchLoop()
  })

// Internal Functions
const requestSuburb = ({ id, lat, lng }) =>
  get(GEOCODE_API, {
    params: {
      key: GEOCODE_KEY,
      latlng: `${lat},${lng}`,
    }
  })
    .then(pipe(
      path(['data','results']),
      map(prop('address_components')),
      flatten,
    ))
    .then(sideFx(addressCmpts => {
      if (!addressCmpts || !addressCmpts.length)
        throw new Error(NO_LATLNG_MATCH_ERR)
    }))
    .then(selectSuburb)
    .then(sideFx(suburb => {
      if (!suburb)
        throw new Error(NO_SUBURB_MATCH_ERR)
    }))
    .then(pipe(
      prop('long_name'),
      objOf('suburb'),
    ))
    .catch(error => {
      // These are non-deterministic errors (i.e. doesn't always end up with same result)
      // so let's just keep on re-requesting when they occur
      const RECON_ERRS = [ENOTFOUND_ERR, NO_LATLNG_MATCH_ERR, SOCKET_HANGUP_ERR]
      if (RECON_ERRS.includes(trimErrMsg(String(error))))
        return requestSuburb({ id, lat, lng })

      if (!errors$)
        errors$ = fs.createWriteStream(`${OUTPUT_DIR}/errors-${timestamp}.txt`)

      errors$.write(`${id}: ${error}`)
      return { error }
    })

const isTypeLocality = pipe(prop('types'), contains('locality'))
const selectSuburb = find(isTypeLocality)

const sideFx = (fn) => (x) => {
  fn(x)
  return x
}

const trimErrMsg = s => s.replace('Error: ', '')

const timezoneToCity = s => s.replace('Australia/', '')
