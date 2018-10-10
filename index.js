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
const suburbIds = []
let isStart = true

let errors$
const input$ = fs.createReadStream(`${INPUT_DIR}/input.json`)
const output$ = fs.createWriteStream(`${OUTPUT_DIR}/output-${timestamp}.json`)

output$.write('[')

oboe(input$)
  .node('{id lat lng}', ({ id, lat, lng, timezone }) => {
    requestsFn.push(
      () => requestSuburb({ id, lat, lng })
        .then(({ suburb, error }) => {
          const city = timezoneToCity(timezone)
          const entryId = `${city}/${suburb}`

          if (suburbIds.includes(entryId))
            return

          suburbIds.push(entryId)

          const dataKey = error ? 'error' : 'suburb'

          if (isStart)
            isStart = false
          else
            output$.write(`,`)

          output$.write(
            '\n' +
            '  {\n' +
            `    "lat": ${lat},\n` +
            `    "lng": ${lng},\n` +
            `    "city": "${city}",\n` +
            `    "${dataKey}": "${error || suburb}"\n` +
            '  }'
          )
        })
        .catch(console.error)
    )
  })
  .done(() => {
    const requestByBatchLoop = () => {
      const batchedRequestsFn = requestsFn.splice(0, REQUEST_COUNT_PER_BATCH)
      const batchedRequestsP = batchedRequestsFn.map(requestFn => requestFn())

      Promise.all(batchedRequestsP)
        .then(() => {
          if (requestsFn.length) {
            setTimeout(requestByBatchLoop, REQUEST_BATCH_INTERVAL)
            return
          }

          output$.write('\n]')
          output$.end()

          if (errors$)
            errors$.end()

          console.timeEnd('fill-up-suburbs-data')
          console.log(`Created ${suburbIds.length} entries`)
        })
    }

    requestByBatchLoop()
  })

// Internal Functions
const requestSuburb = ({ id, lat, lng }) =>
  get(GEOCODE_API, {
    params: {
      key: GEOCODE_KEY,
      latlng: `${lat},${lng}`
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

      errors$.write(`${id}: ${error}\n`)
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
