# yahoo-finance
Get stock data, largest movers, news and event calendars from Yahoo Finance hidden API, without any API key or subscription.

Just download the R file and enjoy the best of Yahoo Finance! 

If you want OHLCV data for e.g. NVDA, type:

```
Yahoo_Finance_Data(Ticker = "NVDA", interval = "15m", Range = "2d", End_Candle = T)
```

If you want OHLCV data for multiple symbols, type:

```
Yahoo_Finance_Multi_Data(Tickers = c("SPY", "TSLA", "NVDA", "AAPL", "AMD", "QQQ", "BA", "GME"), Range = "1y", Interval = "1d", Last_N = 100)
```

For recent news for a symbol, type:

```
Yahoo_News(Ticker = "MSFT")
```

Earnings calendat for today can be obtained with:

```
Yahoo_Events_Calendar(list = "earnings", day = Sys.Date())
```
