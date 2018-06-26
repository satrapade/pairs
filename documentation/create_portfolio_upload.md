# create portfolio position files suitable for uploading into bloomberg using BBU

1. scrape LUKE, DUKE position off the latest sheet
2. setup a mapping


| column name | contents |
|-------------|----------|
|PID | Portfolio Name |
| PNUM | Portfolio Number |
| BENCH | Benchmark Name |
| QUANTITY | Quantity/Positions? |
| FWEIGHT | Fixed Weight for Portfolios and Benchmarks |
| DWEIGHT | Drifting Weight for Portfolios and Benchmarks |
| ID_TYPE | Numeric ID Type |
| DATE | Position Date, For Transactions it's the Trade date |
| EXCHANGE | Two digit Exchange Code |
| ID | Security Id |
| COST | Cost Price |
| COST_XRATE | Cost Exchange Rate |
| USER_PX | User Price |
| USER_MKT_VALUE | User Market Value |

---



