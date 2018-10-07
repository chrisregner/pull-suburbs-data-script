const requestSuburb = (lat, lng) =>
  get(GEOCODE_API, {
    params: {
      key: GEOCODE_KEY,
      latlng: `${lat},${lng}`
    }
  })
    .then(resp => resp.data.results)
    .then(rs => rs.map(r =>
      r.address_components
        .map(pick(['long_name', 'types']))
        .map(a => ({ ...a, types: a.types.filter(t => t !== 'political')}))
        .filter(a =>
          !a.types.some(t =>
            EXCLUDED_TYPES.includes(t)))
        .map(pipe(
          Object.values,
          flatten,
        ))
    ))
    .then(as => JSON.stringify(as))
    .catch(console.error)

const EXCLUDED_TYPES = [
  'street_number',
  'country',
  'postal_code',
]
