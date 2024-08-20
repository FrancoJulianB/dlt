WITH raw_data AS (
  SELECT
    _id,
    name,
    code,
    type,
    prizeType,
    currency,
    CAST(amount AS INT64) AS amount,
    CAST(stock AS INT64) AS stock,
    TIMESTAMP(validityFrom) AS validityFrom,
    TIMESTAMP(validityTo) AS validityTo,
    status,
    referralUser,
    -- Keep redeems as STRING
    redeems AS redeems_string,
    TIMESTAMP(updatedAt) AS updatedAt,
    userList
  FROM `n1u_dataset.referralcodes`
),

-- Expand the redeems array
redeem_expanded AS (
  SELECT
    _id,
    name,
    code,
    type,
    prizeType,
    currency,
    amount,
    stock,
    validityFrom,
    validityTo,
    status,
    referralUser,
    -- Extract individual redeem records
    JSON_EXTRACT_SCALAR(redeem, '$.user') AS redeem_user,
    TIMESTAMP(JSON_EXTRACT_SCALAR(redeem, '$.date')) AS redeem_date,
    updatedAt,
    userList  -- Ensure `userList` is included here
  FROM raw_data
  CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(redeems_string, '$')) AS redeem
)

-- Final output
SELECT
  _id,
  name,
  code,
  type,
  prizeType,
  currency,
  amount,
  stock,
  validityFrom,
  validityTo,
  status,
  referralUser,
  redeem_user,
  redeem_date,
  updatedAt,
  userList
FROM redeem_expanded;
