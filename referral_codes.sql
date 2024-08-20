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
    TIMESTAMP_MILLIS(CAST(validityFrom AS INT64)) AS validityFrom,
    TIMESTAMP_MILLIS(CAST(validityTo AS INT64)) AS validityTo,
    status,
    referralUser,
    redeems,
    TIMESTAMP_MILLIS(CAST(updatedAt AS INT64)) AS updatedAt,
    userList
  FROM `your_project.your_dataset.referral_codes`
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
    redeem.user AS redeem_user,
    TIMESTAMP_MILLIS(CAST(redeem.date AS INT64)) AS redeem_date
  FROM raw_data
  CROSS JOIN UNNEST(redeems) AS redeem
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
