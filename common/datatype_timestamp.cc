
using hyperdex::datatype_info;
using hyperdex::datatype_timestamp;

namespace {



int64_t
unpack(const e::slice& value)
{
  assert(validate(value));

  if (value.empty()) {
    return 0;
  }

  int64_t timestamp;
  e::unpack64le(value.data(),&number);

  return timestamp;
}
}
datatype_timestamp::datatype_timestamp(enum timestamp_interval interval) {
  this.interval = interval;
}

hyperdatatype
datatype_timestamp::datatype()
{
    return HYPERDATATYPE_TIMESTAMP;
}
bool
datatype_timestamp::validate(const e::slice& value) {
  return value.size() == sizeof(int64_t) || value.empty();
}


bool
datatype_timestamp::check_args(const funcall& func)
{
  return func.name == FUNC_SET && func.arg1_datatype == HYPERDATATYPE_TIMESTAMP && validate(func.arg1);
}

uint8_t*
datatype_timestamp::apply(const e::slice& old_value,
                       const funcall* funcs, size_t funcs_sz,
                       uint8_t* writeto)
{
  int64_t timestamp;
  for(size_t i = 0; i < funcs_sz; i++ )
  {
    const funcall * func = funcs +i;
    if (func->name != FUNC_SET) {
      abort();
    }

    timestamp = unpack(func.arg1);
  }

  return e::pack64le(timestamp,writeto);
}

bool
datatype_timestamp::hashable()
{
  return true;
}

const char * INTERVAL_SECOND_STRING = "SECOND";
const char * INTERVAL_MINUTE_STRING = "MINUTE";
const char * INTERVAL_HOUR_STRING = "HOUR";
const char * INTERVAL_DAY_STRING = "DAY";
const char * INTERVAL_WEEK_STRING = "WEEK";
const char * INTERVAL_MONTH_STRING = "MONTH";

const char * interval_to_string(enum timestamp_interval in)
{
  switch(in)
  {
    case SECOND:
      return INTERVAL_SECOND_STRING;
    case MINUTE:
      return INTERVAL_MINUTE_STRING;
    case HOUR:
      return INTERVAL_HOUR_STRING;
    case DAY:
      return INTERVAL_DAY_STRING;
    case WEEK:
      return INTERVAL_WEEK_STRING;
    case MONTH:
      return INTERVAL_MONTH_STRING;
    default:
      abort();
  };
}

uint64_t
datatype_timestamp::hash(const e::slice& value) {

  //TODO better hash
  std::cout << "Hashing with "<<interval_to_string(this.interval) << "\n";
  return CityHash64(value.data(),value.size());

}


bool
datatype_timestamp :: containable()
{
    return true;
}

bool
datatype_timestamp :: step(const uint8_t** ptr,
                       const uint8_t* end,
                       e::slice* elem)
{
    if (static_cast<size_t>(end - *ptr) < sizeof(int64_t))
    {
        return false;
    }

    *elem = e::slice(*ptr, sizeof(int64_t));
    *ptr += sizeof(int64_t);
    return true;
}

uint8_t*
datatype_timestamp :: write(uint8_t* writeto,
                        const e::slice& elem)
{
    memmove(writeto, elem.data(), elem.size());
    return writeto + elem.size();
}

bool
datatype_timestamp :: comparable()
{
    return true;
}

static int
compare(const e::slice& lhs,
        const e::slice& rhs)
{
    int64_t lhsnum = unpack(lhs);
    int64_t rhsnum = unpack(rhs);

    if (lhsnum < rhsnum)
    {
        return -1;
    }
    if (lhsnum > rhsnum)
    {
        return 1;
    }

    return 0;
}

int
datatype_timestamp :: compare(const e::slice& lhs, const e::slice& rhs)
{
    return ::compare(lhs, rhs);
}

static bool
compare_less(const e::slice& lhs,
             const e::slice& rhs)
{
    return compare(lhs, rhs) < 0;
}

datatype_info::compares_less
datatype_timestamp :: compare_less()
{
    return &::compare_less;
}


