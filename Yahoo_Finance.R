#' Get OHLCV data from Yahoo Finance
#'
#' @param Ticker 
#' @param interval 
#' @param Range 
#' @param Count 
#' @param End_Candle 
#' @param prepost 
#' @param Add_Candle_Column 
#' @param Add_Symbol_Column 
#' @param Time_less_curr_time 
#' @param Round_Time_Unit 
#' @param Convert_60M_Candle 
#' @param Smooth_NAs 
#' @param Remove_before_NAs 
#' @param Round_to_two_places 
#' @param API_Version 
#'
#' @return
#' @export
#'
#' @examples
#' Yahoo_Finance_Data(Ticker = "AMD", interval = "15m", Range = "5d", End_Candle = T)

Yahoo_Finance_Data <- function (Ticker, interval = "15m", Range, Count = NULL, End_Candle = T, 
    prepost = F, Add_Candle_Column = T, Add_Symbol_Column = F, 
    Time_less_curr_time = T, Round_Time_Unit = NULL, Convert_60M_Candle = T, 
    Smooth_NAs = F, Remove_before_NAs = F, Round_to_two_places = F, 
    API_Version = 2) 
{
    interval <- tolower(interval)
    Range <- tolower(Range)
    Aggregate <- F
    if (interval %in% c("60m", "1h") & Convert_60M_Candle) {
        interval <- "30m"
        Aggregate <- T
    }
    if (Ticker == "VIX") 
        Ticker <- "^VIX"
    if (Ticker == "^VIX") 
        prepost <- T
    if (Ticker == "VVIX") 
        Ticker <- "^VVIX"
    if (!is.null(Count)) {
        multiplier <- if (!interval == "1h") 
            60 * as.numeric(substr(interval, 1, nchar(interval) - 
                1))
        else 3600
        Start <- as.numeric(Sys.time() - multiplier * Count) %>% 
            round(., digits = 0)
        End <- as.numeric(Sys.time()) %>% round(., digits = 0)
        URL <- sprintf("https://query%s.finance.yahoo.com/v8/finance/chart/%s?formatted=true&crumb=&lang=en-US&region=US&period1=%s&period2=%s&interval=%s&includePrePost=%s", 
            API_Version, Ticker, Start, End, if (interval != 
                "2wk") 
                interval
            else "1wk", prepost) %>% paste0(., "&events=div%7Csplit%7Cearn")
    }
    else {
        URL <- sprintf("https://query%s.finance.yahoo.com/v8/finance/chart/%s?formatted=true&region=US&range=%s&interval=%s&includePrePost=%s", 
            API_Version, Ticker, Range, if (interval != "2wk") 
                interval
            else "1wk", tolower(prepost)) %>% paste0(., "&events=div%7Csplit%7Cearn")
    }
    Stock_Data <- tryCatch(expr = Read_Yahoo_Finance_JSON(JSON = URL, 
        Time_less_curr_time = F), error = function(e) tryCatch(expr = suppressWarnings(readLines(con = URL)) %>% 
        Read_Yahoo_Finance_JSON_Fast(), error = function(e) NULL))
    if (!is.null(Stock_Data)) {
        Stock_Data <- if (Ticker == "^VIX") {
            Stock_Data %>% dplyr::filter(!(High == Low & Volume == 
                0))
        }
        else {
            Stock_Data %>% dplyr::filter(!(High == Low & lubridate::hour(Time) > 
                21))
        }
        multiplier <- if (!interval %in% "1h") 
            60 * as.numeric(substr(interval, 1, nchar(interval) - 
                1))
        else 3600
        if ((interval %in% c("1h", "60m") & Convert_60M_Candle) | 
            Aggregate) 
            multiplier <- 1800
        if (End_Candle & !(interval %in% c("1d", "1wk", "2wk", 
            "1mo"))) {
            Stock_Data <- Stock_Data %>% dplyr::mutate(Time = Time + 
                multiplier)
        }
        if (Time_less_curr_time & !(interval %in% c("1d", "1wk", 
            "2wk", "1mo"))) {
            Round_Time_Unit <- if (is.null(Round_Time_Unit)) 
                toupper(interval)
            else Round_Time_Unit
            if (Round_Time_Unit %in% c("60M", "1H")) 
                Round_Time_Unit <- "30M"
            if (Aggregate) 
                Round_Time_Unit <- "60M"
            Cut_off_time <- lubridate::floor_date(Sys.time() - 
                if (!End_Candle) 
                  multiplier
                else 0, unit = Round_Time_Unit)
            if (!US_Market_Open() & !Ticker %in% c("VIX", "^VIX") & 
                !prepost) 
                Cut_off_time <- Yesterdays_Close()
            Stock_Data <- Stock_Data %>% dplyr::filter(Time <= 
                Cut_off_time)
        }
        if (lubridate::second(Stock_Data$Time %>% last()) != 
            0) 
            Stock_Data <- head(Stock_Data, -1)
        if (Aggregate) 
            Stock_Data <- Stock_Data %>% Aggregate_TS_by_Group_Size(podatki = ., 
                Group_Size = 2, YF_60_Min_Convert = T)
        if (interval == "2wk") 
            Stock_Data <- Stock_Data %>% Aggregate_Multiple_Granularities(Aggregate_Size = 2, 
                Last_N = 9999) %>% .[[ncol(.)]] %>% .[[1]] %>% 
                data.frame()
        if (Add_Candle_Column) {
            Int <- if (Aggregate) 
                "60m"
            else interval
            Stock_Data <- Stock_Data %>% dplyr::mutate(Candle = Int %>% 
                gsub("m$", "_Min", .) %>% gsub("1d", "D", .) %>% 
                gsub("1wk", "W", .) %>% gsub("2wk", "2W", .) %>% 
                gsub("1mo", "M", .))
        }
        if (Add_Symbol_Column) 
            Stock_Data <- Stock_Data %>% dplyr::mutate(Symbol = if (Ticker == 
                "^VIX") 
                "VIX"
            else Ticker)
        if (Smooth_NAs) 
            Stock_Data <- Stock_Data %>% Smooth_NA_Values()
        if (Remove_before_NAs & !Smooth_NAs) {
            if (any(is.na(Stock_Data$Close))) {
                Stock_Data <- Stock_Data[(max(which(is.na(Stock_Data$Close))) + 
                  1):nrow(Stock_Data), ]
            }
        }
        if (Round_to_two_places) {
            Places_to_round <- if (dplyr::last(Stock_Data$Close) >= 
                1) 
                2
            else 4
            for (Col in c("Open", "High", "Low", "Close")) Stock_Data[[Col]] <- round(Stock_Data[[Col]], 
                digits = Places_to_round)
            Stock_Data
        }
        Stock_Data
    }
    Stock_Data
}


#' Get OHLCV data from Yahoo Finance for multiple symbols
#'
#' @param Tickers 
#' @param Range 
#' @param Interval 
#' @param Convert_60M_Candle 
#' @param Time_less_curr_time 
#' @param Round_Time_Unit 
#' @param End_Candle 
#' @param Remove_Today 
#' @param prepost 
#' @param Add_Candle_Column 
#' @param Filter 
#' @param Round_to_two_places 
#' @param Last_N 
#' @param Smooth 
#' @param verbatim 
#'
#' @return
#' @export
#'
#' @examples
#' Yahoo_Finance_Multi_Data(Tickers = c("SPY", "TSLA", "NVDA", "AAPL", "AMD", "QQQ", "BA", "GME"), Range = "1y", Interval = "1d", Last_N = 100)

Yahoo_Finance_Multi_Data <- function (Tickers, Range = "5d", Interval = "5m", Convert_60M_Candle = T, 
    Time_less_curr_time = T, Round_Time_Unit = NULL, End_Candle = T, 
    Remove_Today = TRUE, prepost = FALSE, Add_Candle_Column = T, 
    Filter = T, Round_to_two_places = F, Last_N = 75, Smooth = F, 
    verbatim = F) 
{
    Interval <- tolower(Interval)
    Range <- tolower(Range)
    Aggregate <- F
    if (length(Interval) == 1) {
        if (Interval %in% c("60m", "1h") & Convert_60M_Candle) {
            Interval <- "30m"
            Aggregate <- T
            Range <- gsub(stringr::str_extract_all(Range, "\\(?[0-9,.]+\\)?")[[1]], 
                2 * as.numeric(stringr::str_extract_all(Range, 
                  "\\(?[0-9,.]+\\)?")[[1]]), Range)
        }
    }
    if (!is.na(match("VIX", Tickers))) 
        Tickers[match("VIX", Tickers)] <- "^VIX"
    if (!is.na(match("VVIX", Tickers))) 
        Tickers[match("VVIX", Tickers)] <- "^VVIX"
    pool <- curl::new_pool()
    cb <- function(req, verbatin = verbatim) {
        if (verbatim) 
            cat("done:", req$url, ": HTTP:", req$status, "\n")
        podatki[[match(req$url, uris)]] <<- rawToChar(req$content)
    }
    Combinations <- expand.grid(Tickers, Range, Interval, prepost) %>% 
        dplyr::mutate(Var4 = ifelse(Var1 %in% c("^VIX"), TRUE, 
            Var4))
    uris <- sprintf("https://query2.finance.yahoo.com/v8/finance/chart/%s?formatted=true&crumb=&lang=en-US&region=US&range=%s&interval=%s&includePrePost=%s", 
        Combinations$Var1, Combinations$Var2, if (!"2wk" %in% 
            Interval) 
            Combinations$Var3
        else "1wk", tolower(Combinations$Var4))
    podatki <- vector("list", length = length(uris))
    sapply(uris, curl::curl_fetch_multi, done = cb, pool = pool)
    Start <- Sys.time()
    out <- curl::multi_run(pool = pool)
    if (verbatim) 
        print(Sys.time() - Start)
    if (!is.na(match("^VIX", Tickers))) 
        Tickers[match("^VIX", Tickers)] <- "VIX"
    if (!is.na(match("^VVIX", Tickers))) 
        Tickers[match("^VVIX", Tickers)] <- "VVIX"
    if (length(Interval) > 1) {
        podatki <- podatki %>% set_names(paste0(as.vector(Combinations$Var1), 
            "_", as.vector(Combinations$Var3))) %>% purrr::compact()
    }
    else {
        podatki <- podatki %>% set_names(Tickers) %>% purrr::compact()
    }
    Index <- which(sapply(podatki, jsonlite::validate)) %>% as.vector()
    podatki <- podatki[Index] %>% purrr::map(., ~Read_Yahoo_Finance_JSON_Fast(.x, 
        Time_less_curr_time = F, Round_Time_Unit = NULL)) %>% 
        purrr::compact()
    if (!US_Market_Open()) 
        podatki <- Filter(function(x) nrow(x) > 0, podatki)
    if (length(Interval) > 1) {
        if (all(End_Candle & !Interval %in% c("1d", "1wk", "2wk", 
            "1mo"))) {
            podatki <- podatki %>% purrr::imap(function(.x, .y) {
                multiplier <- if (.y != "1h") 
                  as.numeric(stringr::str_extract(gsub(".*_", 
                    "", .y), "\\-*\\d+\\.*\\d*")) * 60
                else 3600
                if (.y %in% c("60m", "1h")) 
                  multiplier <- 1800
                if (Aggregate) 
                  multiplier <- 1800
                .x$Time <- .x$Time + multiplier
                .x
            })
        }
    }
    else {
        if (End_Candle & !Interval %in% c("1d", "1wk", "2wk", 
            "1mo")) {
            multiplier <- if (Interval != "1h") 
                as.numeric(stringr::str_extract(gsub(".*_", "", 
                  Interval), "\\-*\\d+\\.*\\d*")) * 60
            else 3600
            if (Interval %in% c("60m", "1h") | Aggregate) 
                multiplier <- 1800
            podatki <- lapply(podatki, function(x) {
                x$Time <- x$Time + multiplier
                x
            })
        }
    }
    if (length(podatki) > 0) {
        if (all(Interval %in% c("1d", "1wk", "2wk", "1mo"))) 
            podatki <- purrr::map(podatki, ~.x %>% dplyr::mutate(Time = as.Date(Time)))
        if (all(Time_less_curr_time & !(Interval %in% c("1d", 
            "1wk", "2wk", "1mo")))) {
            if (length(Interval) > 1) {
                podatki <- podatki %>% purrr::imap(function(.x, 
                  .y) {
                  Round_Time_Unit <- if (is.null(Round_Time_Unit)) 
                    toupper(gsub(".*_", "", .y))
                  else Round_Time_Unit
                  if (Round_Time_Unit == "60M") 
                    Round_Time_Unit <- "30M"
                  if (Aggregate) 
                    Round_Time_Unit <- "60M"
                  Cut_off_time <- lubridate::floor_date(Sys.time() - 
                    if (!End_Candle) 
                      multiplier
                    else 0, unit = Round_Time_Unit)
                  .x <- .x[.x$Time <= Cut_off_time, ]
                  if (lubridate::second(tail(.x$Time, 1)) != 
                    0) 
                    .x <- slice(.x, 1:(nrow(.x) - 1))
                  .x
                })
            }
            else {
                Round_Time_Unit <- if (is.null(Round_Time_Unit)) 
                  toupper(Interval)
                else Round_Time_Unit
                if (Round_Time_Unit == "60M") 
                  Round_Time_Unit <- "30M"
                if (Aggregate) 
                  Round_Time_Unit <- "60M"
                Cut_off_time <- lubridate::floor_date(Sys.time() - 
                  if (!End_Candle) 
                    multiplier
                  else 0, unit = Round_Time_Unit)
                podatki <- lapply(podatki, function(x) {
                  x <- x[x$Time <= Cut_off_time, ]
                  if (lubridate::second(last(x$Time)) != 0) 
                    x <- x[-nrow(x), ]
                  x
                })
            }
        }
        podatki <- lapply(podatki, function(x) {
            if (unique(x$Symbol) != "VIX") {
                x$Remove <- (x$High == x$Low & x$Volume == 0)
            }
            else {
                x <- x[!is.na(x$Volume), ]
                x$Remove <- (x$High == x$Low & lubridate::hour(x$Time) > 
                  21)
            }
            x$Remove[is.na(x$Remove)] <- TRUE
            x <- x[!x$Remove, colnames(x) != "Remove"]
            x
        })
        if (Filter) 
            podatki <- podatki %>% Filter_YF_Data(Dataset = ., 
                Smooth = Smooth, Last_N = if (Aggregate) 
                  Last_N * 2
                else Last_N, End_Candle = End_Candle)
        if (Aggregate) 
            podatki <- podatki %>% purrr::map(~.x %>% Aggregate_TS_by_Group_Size(podatki = ., 
                Group_Size = 2, YF_60_Min_Convert = T)) %>% Imap_and_rbind() %>% 
                split(., .$Symbol)
        if (Remove_Today & all(Interval %in% c("1d", "1wk", "2wk", 
            "1mo"))) 
            podatki <- podatki %>% purrr::map(~.x %>% dplyr::filter(as.Date(Time) != 
                Sys.Date()))
        if (identical(Interval, "2wk")) 
            podatki <- podatki %>% Aggregate_Multiple_Granularities(Aggregate_Size = 2, 
                Last_N = 9999) %>% .[[ncol(.)]]
        if (Add_Candle_Column) {
            if (length(Interval) > 1) {
                podatki <- podatki %>% purrr::imap(function(.x, 
                  .y) {
                  if (Aggregate & .y == "30m") 
                    .y <- "60m"
                  Candle <- gsub(".*_", "", .y) %>% gsub("m$", 
                    "_Min", .) %>% gsub("1d", "D", .) %>% gsub("1wk", 
                    "W", .) %>% gsub("2wk", "2W", .) %>% gsub("1mo", 
                    "M", .) %>% gsub("1h", "60_Min", .)
                  .x$Candle <- Candle
                  .x
                })
            }
            else {
                Candle_Stamp <- Interval %>% {
                  if (Aggregate & . == "30m") 
                    "60m"
                  else Interval
                } %>% gsub("m$", "_Min", .) %>% gsub("1d", "D", 
                  .) %>% gsub("1wk", "W", .) %>% gsub("2wk", 
                  "2W", .) %>% gsub("1mo", "M", .) %>% gsub("1h", 
                  "60_Min", .)
                podatki <- purrr::map(podatki, function(x) {
                  x$Candle <- Candle_Stamp
                  x
                })
            }
        }
        if (length(Interval) > 1) 
            names(podatki) <- gsub("_.*", "", names(podatki))
        podatki <- podatki %>% purrr::map(~.x %>% data.table::data.table())
        podatki <- podatki %>% purrr::map(~.x %>% dplyr::ungroup())
        if (Round_to_two_places) {
            podatki <- podatki %>% purrr::map(function(x) {
                Places_to_round <- if (dplyr::last(x$Close) >= 
                  1) 
                  2
                else 4
                for (Col in c("Open", "High", "Low", "Close")) x[[Col]] <- round(x[[Col]], 
                  digits = Places_to_round)
                x
            })
        }
        podatki
    }
    return(podatki)
}


#' Largest Movers
#'
#' @param N Top N
#'
#' @return
#' @export
#' @examples
#' Yahoo_Daily_Movers(N = 20)

Yahoo_Daily_Movers <- function (N = 20) 
{
    Gainers <- jsonlite::fromJSON(txt = paste0("https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=false&lang=en-US&region=US&scrIds=day_gainers&count=", 
        N, "&corsDomain=finance.yahoo.com"))
    Gainers <- Gainers$finance$result$quotes[[1]] %>% dplyr::filter(grepl("Real", 
        quoteSourceName)) %>% dplyr::mutate_at(dplyr::vars(dplyr::ends_with("Time")), 
        function(x) Numeric_to_Date(x))
    Losers <- jsonlite::fromJSON(txt = paste0("https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=false&lang=en-US&region=US&scrIds=day_losers&count=", 
        N, "&corsDomain=finance.yahoo.com"))
    Losers <- Losers$finance$result$quotes[[1]] %>% dplyr::filter(grepl("Real", 
        quoteSourceName)) %>% dplyr::mutate_at(dplyr::vars(dplyr::ends_with("Time")), 
        function(x) Numeric_to_Date(x))
    Movers <- list(Gainers, Losers) %>% set_names(c("Gainers", 
        "Losers")) %>% purrr::map(~.x %>% dplyr::mutate(Last_Update = Sys.time() - 
        regularMarketTime))
    return(Movers)
}


#' News for a symbol
#'
#' @param Ticker 
#' @param count 
#'
#' @return
#' @export
#'
#' @examples
#' Yahoo_News(Ticker = "MSFT")

Yahoo_News <- function (Ticker, count = 50) 
{
    Feed <- suppressWarnings(tidyRSS::tidyfeed(feed = sprintf("http://feeds.finance.yahoo.com/rss/2.0/headline?s=%s&region=US&lang=en-US&count=%s", 
        Ticker, count))) %>% dplyr::select(-c(feed_language, 
        feed_description, feed_link))
    return(Feed)
}


#' Trending symbols
#'
#' @param count 
#'
#' @return
#' @export
#'
#' @examples
Yahoo_Trending_Symbols <- function (count) 
{
    Raw_Data <- jsonlite::fromJSON(txt = sprintf("https://query1.finance.yahoo.com/v1/finance/trending/US?lang=en-US&region=US&count=%s&corsDomain=finance.yahoo.com", 
        count))
    Trending <- Raw_Data$finance$result$quotes[[1]]
    return(Trending)
}


#' Largest volume
#'
#' @return
#' @export
#'
#' @examples
Yahoo_Largest_Volume <- function () 
{
    Raw_Data <- jsonlite::fromJSON(txt = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=false&lang=en-US&region=US&scrIds=most_actives&count=5")
    Stat <- Raw_Data$finance$result$quotes[[1]]
    return(Stat)
}


#' Event Calendars
#'
#' @param list splits/economic/ipo/calendar/earnings
#' @param day 
#'
#' @return
#' @export
#'
#' @examples
Yahoo_Events_Calendar <- function (list = "earnings", day = Sys.Date()) 
{
    if (list == FALSE) {
        list = select.list(c("splits", "economic", "ipo", "calendar", 
            "earnings"))
    }
    HTML <- xml2::read_html(sprintf("https://finance.yahoo.com/calendar/%s?day=%s", 
        list, day))
    i <- 1
    Data <- list()
    Data[[i]] <- HTML %>% rvest::html_nodes(xpath = "//div[@id='cal-res-table']//table") %>% 
        rvest::html_table(header = T, fill = TRUE) %>% .[[1]]
    while (nrow(Data[[i]])%%100 == 0) {
        HTML <- xml2::read_html(sprintf("https://finance.yahoo.com/calendar/%s?day=%s&offset=%s&size=%s", 
            list, day, 100 * i, 100 * i))
        i = i + 1
        Data[[i]] <- HTML %>% rvest::html_nodes(xpath = "//div[@id='cal-res-table']//table") %>% 
            rvest::html_table(header = T, fill = TRUE) %>% .[[1]]
    }
    Data <- do.call("rbind", Data) %>% set_colnames(c(gsub(" ", 
        "_", colnames(.)))) %>% dplyr::filter(EPS_Estimate != 
        "-") %>% dplyr::arrange(.[[ncol(.)]])
    return(Data)
}


#' Upcoming earings for a symbol
#'
#' @param Ticker 
#'
#' @return
#' @export
#'
#' @examples
Yahoo_Upcoming_Earnings_Release <- function (Ticker) 
{
    Table <- xml2::read_html(x = paste0("https://finance.yahoo.com/calendar/earnings?symbol=", 
        Ticker)) %>% rvest::html_table() %>% .[[1]]
    if (length(Table) > 0) {
        Table <- Table %>% dplyr::mutate(Earnings_Day = .[[3]] %>% 
            lubridate::mdy_h() %>% as.Date()) %>% dplyr::select(-3) %>% 
            dplyr::filter(Earnings_Day > Sys.Date()) %>% dplyr::arrange(Earnings_Day)
    }
    Table
}



Read_Yahoo_Finance_JSON_Fast <- function (JSON, Time_less_curr_time = F, Round_Time_Unit = NULL, 
    Remove_NA_Time = F) 
{
    Data <- rjson::fromJSON(json_str = JSON)
    Data <- Data$chart$result[[1]]
    if (!is.null(Data)) {
        if (!is.null(Data$indicators$adjclose)) {
            Conversion_Factor <- tryCatch(expr = Data$indicators$adjclose[[1]]$adjclose/Data$indicators$quote[[1]]$close, 
                error = function(e) rep(1, length(Data$indicators$adjclose[[1]]$adjclose)))
            if (length(Conversion_Factor) == 0) 
                rm(Conversion_Factor)
        }
        if (length(names(Data$indicators$quote[[1]])) == 0) {
            Core_Data <- NULL
        }
        else {
            Core_Data <- Data$indicators$quote[[1]] %>% do.call("cbind", 
                .)
            Core_Data <- cbind(Time = Data$timestamp, Core_Data) %>% 
                data.frame() %>% set_colnames(Correct_Colnames(colnames(.)))
            for (Col in colnames(Core_Data)) {
                if ("list" %in% class(Core_Data[[Col]])) {
                  if (list(NULL) %in% Core_Data[[Col]]) {
                    Core_Data[[Col]][sapply(Core_Data[[Col]], 
                      function(x) length(x) == 0L)] <- NA
                  }
                  Core_Data[[Col]] <- unlist(Core_Data[[Col]])
                }
            }
            Core_Data$Time <- as.POSIXct(Core_Data$Time, origin = "1970-01-01")
            Core_Data <- Core_Data[, c("Time", "Open", "High", 
                "Low", "Close", "Volume")]
            Core_Data$Symbol <- gsub("\\^", "", Data$meta$symbol)
        }
        if (exists("Conversion_Factor")) {
            if (length(Conversion_Factor) > 0 & length(Conversion_Factor) == 
                nrow(Core_Data)) {
                if (!all(na.omit(Conversion_Factor) == 1)) {
                  for (Col in intersect(colnames(Core_Data), 
                    c("Open", "High", "Low", "Close"))) Core_Data[[Col]] <- Core_Data[[Col]] * 
                    Conversion_Factor
                }
                Core_Data
            }
            else if (length(Conversion_Factor) > 0) {
                Conversion_Factor <- head(Conversion_Factor, 
                  -1)
                for (Col in intersect(colnames(Core_Data), c("Open", 
                  "High", "Low", "Close"))) Core_Data[[Col]] <- Core_Data[[Col]] * 
                  Conversion_Factor
            }
        }
        if (Time_less_curr_time) {
            Round_Time_Unit <- if (is.null(Round_Time_Unit)) 
                "1M"
            else Round_Time_Unit
            Max_Time <- lubridate::floor_date(Sys.time(), unit = Round_Time_Unit)
            Core_Data <- Core_Data[Core_Data$Time <= Max_Time, 
                ]
        }
        if (Remove_NA_Time) 
            Core_Data <- Core_Data[!is.na(Core_Data$Time), ]
        Core_Data
    }
    else {
        NULL
    }
}


Read_Yahoo_Finance_JSON <- function (JSON, Time_less_curr_time = F, Round_Time_Unit = NULL, 
    Remove_NA_Time = F) 
{
    Data <- jsonlite::fromJSON(txt = JSON, simplifyDataFrame = T)
    Data <- Data$chart$result
    if (!is.null(Data)) {
        if (!is.null(Data$indicators$adjclose)) {
            Conversion_Factor <- Data$indicators$adjclose[[1]]$adjclose[[1]]/Data$indicators$quote[[1]]$close[[1]]
            if (length(Conversion_Factor) == 0) 
                rm(Conversion_Factor)
        }
        if (length(names(Data$indicators$quote[[1]])) == 0) {
            Core_Data <- NULL
        }
        else {
            Core_Data <- purrr::map(Data$indicators$quote[[1]], 
                function(x) x[[1]]) %>% do.call("cbind", .)
            Core_Data <- cbind(Time = Data$timestamp[[1]], Core_Data) %>% 
                data.frame() %>% set_colnames(Correct_Colnames(colnames(.)))
            Core_Data$Time <- as.POSIXct(Core_Data$Time, origin = "1970-01-01")
            Core_Data <- Core_Data[, c("Time", "Open", "High", 
                "Low", "Close", "Volume")]
            Core_Data <- Core_Data[Core_Data$Time < Sys.time(), 
                ]
            Core_Data$Symbol <- gsub("\\^", "", Data$meta$symbol)
        }
        if (exists("Conversion_Factor")) {
            if (length(Conversion_Factor) > 0 & length(Conversion_Factor) == 
                nrow(Core_Data)) {
                if (!all(na.omit(Conversion_Factor) == 1)) 
                  Core_Data <- Core_Data %>% dplyr::mutate_at(-c(1, 
                    ncol(.)), list(~. * Conversion_Factor))
                Core_Data
            }
            else if (length(Conversion_Factor) > 0) {
                Conversion_Factor <- head(Conversion_Factor, 
                  -1)
                Core_Data %<>% dplyr::mutate_at(dplyr::vars(Open:Close), 
                  list(~. * Conversion_Factor))
            }
        }
        if (Time_less_curr_time) {
            Round_Time_Unit <- if (is.null(Round_Time_Unit)) 
                "1M"
            else Round_Time_Unit
            Max_Time <- lubridate::floor_date(Sys.time(), unit = Round_Time_Unit)
            Core_Data <- Core_Data[Core_Data$Time <= Max_Time, 
                ]
        }
        if (Remove_NA_Time) 
            Core_Data <- Core_Data[!is.na(Core_Data$Time), ]
        Core_Data
    }
    else {
        NULL
    }
}


#' Aggregate from smaller to larger granularity
#'
#' @param dataset 
#' @param Aggregate_Size 
#' @param Periodicity 
#' @param Nrow 
#' @param Last_N 
#' @param End_Candle 
#'
#' @return
#' @export
#'
#' @examples
Aggregate_Multiple_Granularities <- function (dataset, Aggregate_Size, Periodicity = NULL, Nrow = 40, 
    Last_N = 201, End_Candle = TRUE) 
{
    if ("list" %in% class(dataset)) {
        if (length(names(dataset)) == length(dataset)) {
            dataset <- Convert_List_to_Nested_Tibble(List = dataset)
        }
        else {
            stop("Dataset must either be nested Tibble or list with names!")
        }
    }
    else if ("tbl_df" %in% class(dataset)) {
        Symbol_Column <- Determine_Symbol_Column(Dataset = dataset)
        if (!("data" %in% names(dataset))) 
            dataset <- dataset %>% dplyr::group_by(!!as.name(Symbol_Column)) %>% 
                tidyr::nest()
        colnames(dataset) <- c("Symbol", "data")
        if (is.null(names(dataset$data))) 
            names(dataset$data) <- dataset$Symbol
        if (length(names(dataset$data)) != length(dataset$data)) 
            stop("Data column has no names!")
    }
    else {
        Symbol_Column <- Determine_Symbol_Column(Dataset = dataset)
        dataset <- dataset %>% dplyr::group_by(!!as.name(Symbol_Column)) %>% 
            tidyr::nest()
    }
    if (Aggregate_Size[1] != 0) {
        Aggregate_Size <- c(1, Aggregate_Size)
        if (is.null(Periodicity)) 
            Periodicity <- dataset$data[[1]]$Time %>% xts::periodicity()
        if (grepl("min|M", Periodicity$label, ignore.case = T)) {
            Colnames <- paste0(Periodicity$frequency * Aggregate_Size, 
                "_", stringi::stri_trans_totitle(Periodicity$label)) %>% 
                gsub("ute", "", .)
            Colnames[startsWith(Colnames, "-")] <- "D"
        }
        else if (grepl("sec|S", Periodicity$label, ignore.case = T)) {
            Colnames <- paste0(Periodicity$frequency * Aggregate_Size, 
                "_", stringi::stri_trans_totitle(Periodicity$label)) %>% 
                gsub("ond", "", .) %>% gsub("60_Sec", "1_Min", 
                .) %>% gsub("300_Sec", "5_Min", .)
            Colnames[startsWith(Colnames, "-")] <- "D"
        }
        else if (grepl("day|D", Periodicity$label, ignore.case = T)) {
            Colnames <- paste0(((Periodicity$frequency)/86400) * 
                Aggregate_Size, "_", stringi::stri_trans_totitle(Periodicity$label)) %>% 
                gsub("1_Day", "D", .) %>% gsub("5_Day", "W", 
                .)
            Colnames[startsWith(Colnames, "-")] <- "D"
        }
        else if (grepl("week|W", Periodicity$label, ignore.case = T)) {
            Colnames <- paste0(((Periodicity$frequency)/604800) * 
                Aggregate_Size, "_", stringi::stri_trans_totitle(Periodicity$label))
            Colnames[startsWith(Colnames, "-")] <- "D"
        }
        Aggregate_Size <- Aggregate_Size[-1]
        dataset <- suppressMessages(purrr::map_dfc(Aggregate_Size, 
            function(x) {
                Price_Data_2 <- purrr::map(dataset$data, ~Aggregate_TS_by_Group_Size(.x, 
                  Group_Size = x, End_Candle = End_Candle)) %>% 
                  purrr::imap(~cbind(.x, Symbol = .y)) %>% dplyr::bind_rows() %>% 
                  dplyr::group_by(Symbol) %>% tidyr::nest()
                return(Price_Data_2)
            }) %>% dplyr::bind_cols(dataset, .) %>% dplyr::select(1, 
            dplyr::contains("data")) %>% set_colnames(c("Symbol", 
            Colnames)))
        if ("Candle" %in% colnames(dataset[[2]][[1]])) {
            for (i in 2:ncol(dataset)) dataset[[i]] <- dataset[[i]] %>% 
                purrr::map(~.x %>% dplyr::mutate(Candle = colnames(dataset)[i]))
        }
        dataset
    }
    else {
        if (missing("Periodicity")) 
            Periodicity <- dataset$data[[1]]$Time %>% xts::periodicity()
        if (Periodicity$label == "minute") {
            Colnames <- paste0(Periodicity$frequency, "_", stringi::stri_trans_totitle(Periodicity$label)) %>% 
                gsub("ute", "", .)
            Colnames[startsWith(Colnames, "-")] <- "D"
        }
        else if (grepl("sec|S|S", Periodicity$label, ignore.case = T)) {
            Colnames <- paste0(Periodicity$frequency, "_", stringi::stri_trans_totitle(Periodicity$label)) %>% 
                gsub("ond", "", .) %>% gsub("60_Sec", "1_Min", 
                .) %>% gsub("300_Sec", "5_Min", .)
            Colnames[startsWith(Colnames, "-")] <- "D"
        }
        else {
            Colnames <- paste0(((Periodicity$frequency)/3600), 
                "_", stringi::stri_trans_totitle(Periodicity$label))
            Colnames[startsWith(Colnames, "-")] <- "D"
        }
        colnames(dataset)[2] <- Colnames
    }
    dataset <- dataset[which(dataset %>% .[, ncol(.)] %>% .[[1]] %>% 
        purrr::map(., ~.x %>% nrow()) >= Nrow) %>% as.vector(), 
        ]
    dataset <- dataset %>% dplyr::mutate_at(-1, ~purrr::map(., 
        function(x) x <- utils::tail(x, n = min(nrow(x), Last_N))))
    for (i in 2:ncol(dataset)) names(dataset[[i]]) <- dataset$Symbol
    dataset
}



Aggregate_TS_by_Group_Size <- function (podatki, Group_Size, End_Candle = TRUE, Remove_First = FALSE, 
    YF_60_Min_Convert = F) 
{
    if (Group_Size > 0) {
        if (!YF_60_Min_Convert) {
            podatki$Group <- rev(rep(1:Group_Size, (nrow(podatki)/Group_Size) + 
                1)[1:nrow(podatki)])
        }
        else {
            Convert_YF_Candle <- function(dataset) {
                dataset$Day <- as.Date(dataset$Time)
                dataset <- dataset %>% split(., .$Day) %>% lapply(function(x) {
                  N_row <- nrow(x)
                  x$Group <- rev(rep(1:2, each = 1, times = ceiling(N_row/2))[1:N_row])
                  x
                }) %>% dplyr::bind_rows()
                Min_Day <- min(dataset$Day)
                dataset <- dataset[dataset$Day != Min_Day, ]
                dataset <- dataset[, colnames(dataset) != "Day"]
                dataset
            }
            podatki <- Convert_YF_Candle(dataset = podatki)
        }
        podatki$Sum <- rev(cumsum(rev(podatki$Group) == 1))
        podatki <- podatki %>% na.omit() %>% dplyr::group_by(Sum)
        Match_Group <- if (End_Candle) 
            1
        else Group_Size
        Aggregated_TS <- podatki %>% dplyr::mutate(Open = dplyr::first(Open), 
            High = max(High), Low = min(Low), Close = dplyr::last(Close), 
            Volume = sum(Volume)) %>% dplyr::ungroup() %>% dplyr::select(-Sum) %>% 
            dplyr::filter(Group == Match_Group)
        Aggregated_TS <- Aggregated_TS[, c("Time", "Open", "High", 
            "Low", "Close", "Volume")]
    }
    else {
        Aggregated_TS <- podatki %>% dplyr::group_by(Group = as.Date(.[[1]])) %>% 
            dplyr::summarise(Open = dplyr::first(Open), High = max(High), 
                Low = min(Low), Close = dplyr::last(Close), Volume = sum(Volume), 
                .groups = "keep") %>% dplyr::ungroup() %>% dplyr::arrange(Group) %>% 
            dplyr::rename(Time = Group)
        Aggregated_TS
    }
    Aggregated_TS
}


Smooth_NA_Values <- function (dataset) 
{
    nmissing <- function(x) sum(is.na(x))
    NAs <- (plyr::colwise(nmissing))(dataset)
    Repair <- which(NAs > 0)
    if ("Symbol" %in% colnames(dataset)) 
        dataset <- dataset %>% dplyr::group_by(Symbol)
    if (length(Repair) > 0) {
        for (i in Repair) dataset[, i] <- zoo::na.spline(dataset[, 
            i])
    }
    if ("Symbol" %in% colnames(dataset)) 
        dataset <- dataset %>% dplyr::ungroup()
    dataset
}


US_Market_Open <- function (Allowed_Extra = NULL) 
{
    if (!weekdays(Sys.Date()) %in% c("sobota", "nedelja", "sunday", 
        "saturday")) {
        RGM <- US_Market_Open_and_Close_Hour()
        Open_Day <- !Market_Holidays()
        if (!is.null(Allowed_Extra)) 
            RGM$Close <- RGM$Close + tail(Allowed_Extra, 1)
        Market_Open <- Sys.time() >= (RGM$Open) & Sys.time() <= 
            (RGM$Close) & Open_Day
        return(Market_Open)
    }
    else {
        return(FALSE)
    }
}


Yesterdays_Close <- function () 
{
    Base <- Base_Folder(Suffix = "", Root = TRUE)
    File <- paste0(Base, "IB_Additional_Data/Last_Close_", Correct_Colnames(Sys.Date()), 
        ".csv")
    Update <- T
    if (file.exists(File)) {
        if (file.info(File)$mtime >= Sys.time() - 2 * 3600) {
            Yesterday_Close <- data.table::fread(file = File)[[1]]
            if (!is.na(Yesterday_Close)) 
                Update <- F
            if (file.info(File)$mtime < US_Market_Open_and_Close_Hour(Time = "Close") + 
                10 * 60 & Sys.time() > US_Market_Open_and_Close_Hour(Time = "Close") + 
                10 * 60) {
                Update <- T
            }
        }
    }
    if (Update) {
        list.files(path = paste0(Base, "IB_Additional_Data/"), 
            pattern = "^Last_Close", full.names = T) %>% file.remove
        Yesterday_Close <-             R.utils::withTimeout({
          CNBC_Intraday_Data_Fast(Ticker = "AMD", N_Days = 5, 
                                  Interval = "30M", End_Candle = T) %>% .[[1]] 
        }, timeout = 5, onTimeout = "silent")
        if (is.null(Yesterday_Close)) {
            Yesterday_Close <- CNBC_Intraday_Data(Ticker = "AMD", 
                N_Days = 5, Interval = "5M") %>% .[[1]] 
        }
        Yesterday_Close <- Yesterday_Close %>% dplyr::filter(as.Date(Time) <= 
            Read_last_Business_Day()) %>% tail(1) %>% .$Time
        if (as.Date(Yesterday_Close) %in% Half_Open_Days()) 
            Yesterday_Close <- as.POSIXct(paste(Read_last_Business_Day(), 
                "19:00:00"))
        data.table::fwrite(x = Yesterday_Close %>% DF, file = File, 
            quote = F, row.names = F)
    }
    Yesterday_Close <- data.table::fread(file = File)[[1]]
    if (is.na(Yesterday_Close)) {
        Hour <- if (as.Date(Read_last_Business_Day()) %in% Half_Open_Days()) 
            13
        else 16
        Yesterday_Close <- as.POSIXct(paste(Read_last_Business_Day(), 
            paste0(Hour + Time_Diff_Ljubljana_New_York(), ":00:00")))
    }
    if (lubridate::tz(x = Yesterday_Close) == "UTC") 
        Yesterday_Close <- Yesterday_Close %>% Convert_Time_Zones(From = "UTC")
    Yesterday_Close
}


Convert_List_to_Nested_Tibble <- function (List, Group_By_Column = "Symbol") 
{
    if (any(c("Ticker", "Symbol") %in% colnames(List[[1]]))) {
        if ("Ticker" %in% colnames(List[[1]])) 
            List <- List %>% dplyr::rename(Symbol = Ticker)
        List <- List %>% dplyr::bind_rows() %>% dplyr::group_by(Symbol) %>% 
            tidyr::nest()
    }
    else {
        List <- List %>% purrr::imap(~cbind(.x, Symbol = .y)) %>% 
            dplyr::bind_rows() %>% dplyr::group_by(Symbol) %>% 
            tidyr::nest()
    }
    names(List$data) <- List$Symbol
    List
}


Determine_Symbol_Column <- function (Dataset) 
{
    Symbol_Column <- intersect(c("Ticker", "Symbol", "ticker", 
        "symbol"), colnames(Dataset))
    stopifnot(length(Symbol_Column) > 0)
    if (length(Symbol_Column) > 1) 
        Symbol_Column <- "Symbol"
    Symbol_Column
}


Correct_Colnames <- function (Colnames) 
{
    Colnames <- Colnames %>% stringi::stri_trans_totitle() %>% 
        gsub("\\.| |-|\\,|\\(|\\)|/|\\'|\\:|\\|", "_", .) %>% 
        gsub("__", "_", .) %>% gsub("_$|\\^", "", .) %>% gsub("&", 
        "and", .) %>% gsub("\\s+", "_", .)
    Colnames
}



Market_Holidays <- function (True_False = TRUE, Date = NULL, Country = "United States") 
{
    File <- Base_Folder(Root = T) %>% paste0("IB_Additional_Data/Additional_CSVs/Holidays.csv")
    if (!file.exists(File)) {
        Update <- T
    }
    else {
        Info <- file.info(File)
        Update <- if (difftime(Sys.Date(), Info$mtime %>% as.Date(), 
            units = "days") > 5) 
            TRUE
        else FALSE
    }
    if (Update) {
        Market_Holidays <- Investing_Com_Holiday_Calendar() %>% 
            set_colnames(c("Date", "Country", "Exchange", "Holiday")) %>% 
            dplyr::filter(!grepl("Early Close", Holiday, ignore.case = T))
        write.table(x = Market_Holidays, file = File, quote = F, 
            row.names = F, sep = ";")
    }
    Holidays <- data.table::fread(file = File, sep = ";")
    if (True_False) {
        Day <- if (is.null(Date)) 
            Sys.Date()
        else Date
        Today <- unlist(Holidays[Holidays$Date == Day, "Country"], 
            use.names = F)
        US_Market_Open <- if (Country %in% Today) 
            TRUE
        else FALSE
        if (Country == "United States") 
            writeLines(text = as.character(US_Market_Open), con = Base_Folder(Root = T) %>% 
                paste0("IG_Code_and_Documents/Confirmation_Txt_Files/Today_Holiday.txt"))
        US_Market_Open
    }
    else {
        Holidays <- Holidays %>% set_colnames(c("Date", "Country", 
            "Exchange", "Holiday"))
        if (!is.null(Date)) 
            Holidays <- Holidays %>% dplyr::filter(Date == !!Date)
        Holidays
    }
}


US_Market_Open_and_Close_Hour <- function (Time, Last_Close = F) 
{
    Time_Diff <- Time_Diff_Ljubljana_New_York()
    if (!Last_Close) {
        Open <- as.POSIXct(paste0(Sys.Date(), " ", round(9 + 
            Time_Diff, digits = 0), ":30:00"))
        Hour <- if (!Sys.Date() %in% Half_Open_Days()) 
            16
        else 13
        Close <- as.POSIXct(paste0(Sys.Date(), " ", round(Hour + 
            Time_Diff, digits = 0), ":00:00"))
        Times <- list(Open = Open, Close = Close)
        if (!missing("Time")) 
            return(Times[[Time]])
        else return(Times)
    }
    else {
        Yesterdays_Close()
    }
}


Base_Folder <- function (Suffix, Root = FALSE) 
{
   getwd()
}


#' Get OHLCV data for multiple symbols from CNBC 
#'
#' @param Ticker 
#' @param N_Days 
#' @param From 
#' @param Interval 
#' @param Extended_Hours 
#' @param End_Candle 
#' @param Filter_Gaps 
#' @param Round_to_two_places 
#' @param Add_Candle_Column 
#' @param ... 
#'
#' @return
#' @export
#'
#' @examples
#' CNBC_Intraday_Data(Ticker = "NVDA", N_Days = 10, Interval = "1D")
#' CNBC_Intraday_Data(Ticker = c("SPY", "AAPL", "BA"), N_Days = 10, Interval = "1D")


CNBC_Intraday_Data <- function (Ticker, N_Days, From = NULL, Interval = "30M", Extended_Hours = FALSE, 
    End_Candle = T, Filter_Gaps = T, Round_to_two_places = F, 
    Add_Candle_Column = T, ...) 
{
    if (identical(Ticker, "Btc")) 
        Ticker <- "BTC.BS="
    if (missing("Extended_Hours")) {
        if (identical(Ticker, "VIX")) 
            Extended_Hours <- T
    }
    Continent <- if (identical(Ticker, ".KS200")) 
        "Asia"
    else if (any(grepl(Ticker, pattern = "-GB|-NL|-DK|-GR|-PT|-BE|-FI|-DE|-FR|-IT|-SE")) | 
        any(startsWith(x = Ticker, prefix = "."))) 
        "Europe"
    else "America"
    if (is.null(From)) 
        From <- paste0(gsub("-", "", Sys.Date() - N_Days), "000000")
    To <- paste0((Sys.Date() + 1) %>% gsub("-", "", .), "000000")
    Ticker <- Ticker %>% gsub("BRK-B", "BRK.B", .) %>% gsub("RDS-B", 
        "RDS.B", .)
    if (Interval %in% c("15M", "2W")) {
        Interval <- if (Interval == "15M") 
            "5M"
        else "1W"
        Aggregate_By <- TRUE
    }
    else {
        Aggregate_By <- FALSE
    }
    Stock_Data <- Curl_Multi_Json_Data(URL_Prefix = "https://ts-api.cnbc.com/harmony/app/bars/", 
        Variable = Ticker, URL_Suffix = paste0("/", Interval, 
            "/", From, "/", To, "/adjusted/EST5EDT.json"), Names = TRUE) %>% 
        purrr::keep(~!(.x %>% .$barData %>% .$priceBars %>% nrow %>% 
            is.null)) %>% purrr::map(~.x %>% .$barData %>% .$priceBars)
    Stock_Data <- Stock_Data %>% purrr::imap(function(.x, .y) {
        .x$tradeTime <- lubridate::ymd_hms(.x$tradeTime)
        test <- .x %>% dplyr::select(tradeTime, open, high, low, 
            close, volume) %>% set_colnames(c("Time", "Open", 
            "High", "Low", "Close", "Volume")) %>% dplyr::mutate_at(-1, 
            as.numeric)
        if (!Interval %in% c("1D", "1W", "2W", "1MO")) {
            if (!Extended_Hours) {
                if (Continent == "America") {
                  Late <- 16
                  if (.y != "VIX") {
                    Early <- 9
                    test <- test %>% dplyr::mutate(Hour = lubridate::hour(Time), 
                      Minute = lubridate::minute(Time))
                    test <- if (Interval != "60M") 
                      test %>% dplyr::filter((Hour < Late & Hour > 
                        Early) | ((Hour >= Early & Hour < Early + 
                        1) & Minute > 29))
                    else test %>% dplyr::filter((Hour < Late & 
                      Hour > Early) | ((Hour >= Early & Hour < 
                      Early + 1) & Minute >= 0))
                    if (min(as.Date(test$Time)) <= as.Date("2020-12-24")) 
                      test <- test %>% dplyr::filter(!(as.Date(test$Time) %in% 
                        c(as.Date("2020-11-27"), as.Date("2020-12-24")) & 
                        Hour >= 13))
                  }
                  test$Time <- test$Time %>% Convert_Time_Zones(From = "America/New_York", 
                    To = "Europe/Ljubljana")
                }
                else if (Continent == "Europe") {
                  test$Time <- test$Time %>% Convert_Time_Zones(From = "America/New_York", 
                    To = "Europe/Ljubljana")
                  test <- test[format(test$Time, format = "%H:%M:%S") >= 
                    "09:00:00", ]
                  test <- test[format(test$Time, format = "%H:%M:%S") <= 
                    "17:29:00", ]
                  if (min(as.Date(test$Time)) <= as.Date("2020-12-24")) {
                    test <- test %>% dplyr::mutate(Hour = lubridate::hour(Time))
                    test <- test %>% dplyr::filter(!(as.Date(test$Time) %in% 
                      c(as.Date("2020-12-24"), as.Date("2020-12-31")) & 
                      Hour > 7))
                  }
                  test
                }
                else if (Continent == "Asia") {
                  test$Time <- test$Time %>% Convert_Time_Zones(From = "America/New_York", 
                    To = "Europe/Ljubljana")
                  test <- test[format(test$Time, format = "%H:%M:%S") >= 
                    "01:00:00", ]
                  test <- test[format(test$Time, format = "%H:%M:%S") <= 
                    "07:30:00", ]
                  test
                }
                test
            }
            else {
                test$Time <- test$Time %>% Convert_Time_Zones(From = "America/New_York", 
                  To = "Europe/Ljubljana")
            }
            test
        }
        else {
            test$Time <- test$Time %>% Convert_Time_Zones(From = "America/New_York", 
                To = "Europe/Ljubljana")
        }
        test <- test[, c("Time", "Open", "High", "Low", "Close", 
            "Volume")]
        if (all(is.na(test$Volume))) 
            test$Volume <- 0
        test
    })
    Stock_Data <- purrr::imap(Stock_Data, function(x, y) {
        if (y != "VIX") {
            x <- x %>% dplyr::filter(!(High == Low & Volume == 
                0))
        }
        else {
            if (!Interval %in% c("1D", "1W", "2W", "1MO")) {
                if (!(Aggregate_By & Interval == "5M")) {
                  x <- x %>% dplyr::filter(!(High == Low & (lubridate::hour(Time) > 
                    21) | lubridate::hour(Time) < 9))
                }
                else {
                  x <- x %>% dplyr::filter(!(High == Low & ((lubridate::hour(Time) > 
                    21) & lubridate::minute(Time) == 15) | lubridate::hour(Time) < 
                    9))
                }
            }
        }
        x
    })
    if (End_Candle & !Interval %in% c("1D", "1W", "2W", "1MO")) {
        Add <- if (grepl("M", Interval)) 
            gsub("M", "", Interval) %>% as.numeric()
        else 60
        Stock_Data <- purrr::map(Stock_Data, ~.x %>% dplyr::mutate(Time = Time + 
            60 * Add))
    }
    if (Filter_Gaps & !Interval %in% c("1D", "1W", "2W", "1MO")) {
        Allowed_Gap <- if (Interval != "1H") 
            as.numeric(gsub("M", "", Interval)) * 3
        else as.numeric(gsub("H", "", Interval)) * 3 * 60
        Symbols_with_less_than_2_gaps <- names(Stock_Data %>% 
            purrr::map(~.x %>% .$Time %>% diff %>% .[. < Allowed_Gap] %>% 
                unique %>% length) %>% unlist() %>% .[. <= if (!Interval %in% 
            c("60M", "1H")) 2 else 3])
        Stock_Data <- Stock_Data[Symbols_with_less_than_2_gaps]
    }
    if (length(Stock_Data) > 0) {
        Stock_Data <- if (Interval %in% c("1D", "1W", "2W", "1MO")) {
            Unit <- lubridate::floor_date(if (!lubridate::wday(Sys.Date()) %in% 
                c(7, 1)) 
                Sys.Date()
            else Last_Friday() + 3, unit = if (Interval != "1MO") 
                Interval %>% gsub("2W", "W", .)
            else "month")
            purrr::map(Stock_Data, function(x) x[as.Date(x$Time) <= 
                Unit, ])
        }
        else {
            Unit <- if (Aggregate_By) 
                (if (Interval == "5M") 
                  "15M"
                else if (Interval == "30M") 
                  "60M")
            else Interval
            purrr::map(Stock_Data, function(x) if (End_Candle) 
                x[x$Time <= lubridate::floor_date(x = Sys.time(), 
                  unit = Unit), ]
            else x[x$Time < lubridate::floor_date(x = Sys.time(), 
                unit = Unit), ])
        }
        if (Aggregate_By) 
            Stock_Data <- Stock_Data %>% Aggregate_Multiple_Granularities(Aggregate_Size = if (Interval == 
                "5M") 
                3
            else 2, Last_N = 100000, Nrow = 5, End_Candle = End_Candle) %>% 
                .[[ncol(.)]]
        if (Add_Candle_Column) 
            Stock_Data <- purrr::map(Stock_Data, ~.x %>% dplyr::mutate(Candle = ifelse(Aggregate_By, 
                if (Interval == "5M") 
                  "15_Min"
                else if (Interval == "30M") 
                  "60_Min"
                else "2W", gsub("M$", "_Min", Interval) %>% gsub("1D", 
                  "D", .) %>% gsub("1W", "W", .) %>% gsub("1MO", 
                  "M", .) %>% gsub("1H$", "60_Min", .))))
        if (Interval %in% c("1D", "1W", "2W", "1MO")) 
            Stock_Data <- purrr::map(Stock_Data, ~.x %>% dplyr::mutate(Time = as.Date(Time)))
        Stock_Data <- purrr::map(Stock_Data, ~.x %>% data.table::data.table())
        if (Round_to_two_places) {
            Stock_Data <- Stock_Data %>% purrr::map(function(x) {
                Places_to_round <- if (dplyr::last(x$Close) >= 
                  1) 
                  2
                else 4
                for (Col in c("Open", "High", "Low", "Close")) x[[Col]] <- round(x[[Col]], 
                  digits = Places_to_round)
                x
            })
        }
        if (Continent == "Europe") {
            if (Interval %in% c("1W", "2W")) {
                if (".STOXX50E" %in% names(Stock_Data)) {
                  if (last(Stock_Data[[".STOXX50E"]]$Time) != 
                    as.Date(purrr::map(Stock_Data, ~.x %>% .$Time %>% 
                      last) %>% unlist(use.names = F) %>% max(), 
                      origin = "1970-01-01")) 
                    Stock_Data[[".STOXX50E"]]$Time <- Stock_Data[[".STOXX50E"]]$Time + 
                      if (Interval == "1W") 
                        7
                      else if (Interval == "2W") 
                        14
                }
            }
        }
    }
    Stock_Data
}


#' Faster version of OHLCV data from CNBC
#'
#' @param Ticker 
#' @param N_Days 
#' @param From 
#' @param Interval 
#' @param Extended_Hours 
#' @param End_Candle 
#' @param Filter_Gaps 
#' @param Round_to_two_places 
#' @param Only_full_candles 
#' @param Add_Candle_Column 
#' @param Continent 
#' @param ... 
#'
#' @return
#' @export
#'
#' @examples
#' CNBC_Intraday_Data_Fast(Ticker = "NVDA", N_Days = 10, Interval = "1D")
#' CNBC_Intraday_Data_Fast(Ticker = c("NVDA","SPY", "BA"), N_Days = 10, Interval = "1D")

CNBC_Intraday_Data_Fast <- function (Ticker, N_Days, From = NULL, Interval = "30M", Extended_Hours = FALSE, 
    End_Candle = T, Filter_Gaps = T, Round_to_two_places = F, 
    Only_full_candles = T, Add_Candle_Column = T, Continent = NULL, 
    ...) 
{
    if (identical(Ticker, "Btc")) 
        Ticker <- "BTC.BS="
    if (missing("Extended_Hours")) {
        if (identical(Ticker, "VIX")) 
            Extended_Hours <- T
    }
    if (is.null(Continent)) {
        Continent <- if (identical(Ticker, ".KS200")) 
            "Asia"
        else if (any(grepl(Ticker, pattern = "-GB|-NL|-DK|-GR|-PT|-BE|-FI|-DE|-FR|-IT|-SE")) | 
            any(startsWith(x = Ticker, prefix = "."))) 
            "Europe"
        else "America"
    }
    if (is.null(From)) 
        From <- paste0(gsub("-", "", Sys.Date() - N_Days), "000000")
    To <- paste0((Sys.Date() + 1) %>% gsub("-", "", .), "000000")
    Ticker <- Ticker %>% gsub("BRK-B", "BRK.B", .) %>% gsub("RDS-B", 
        "RDS.B", .)
    if (Interval %in% c("15M", "2W")) {
        Interval <- if (Interval == "15M") 
            "5M"
        else "1W"
        Aggregate_By <- TRUE
    }
    else {
        Aggregate_By <- FALSE
    }
    Stock_Data <- Curl_Multi_Json_Data(URL_Prefix = "https://ts-api.cnbc.com/harmony/app/bars/", 
        Variable = Ticker, URL_Suffix = paste0("/", Interval, 
            "/", From, "/", To, "/adjusted/EST5EDT.json"), Names = TRUE) %>% 
        purrr::keep(~!(.x %>% .$barData %>% .$priceBars %>% nrow %>% 
            is.null)) %>% purrr::map(~.x %>% .$barData %>% .$priceBars)
    Stock_Data <- Stock_Data %>% purrr::imap(function(.x, .y) {
        .x$tradeTime <- Numeric_to_Date(.x$tradeTimeinMills, 
            Divisor = 1000)
        .x <- .x[, !(colnames(.x) == "tradeTimeinMills")]
        .x <- .x[, c("tradeTime", "open", "high", "low", "close", 
            "volume")]
        .x <- set_colnames(.x, c("Time", "Open", "High", "Low", 
            "Close", "Volume"))
        for (Col in colnames(.x)[-1]) .x[, Col] <- as.numeric(.x[, 
            Col])
        test <- .x
        if (!Interval %in% c("1D", "1W", "2W", "1MO")) {
            if (!Extended_Hours) {
                if (Continent == "America") {
                  Early <- 9
                  Late <- 16
                  if (attributes(test$Time)$tzone != "America/New_York") 
                    test$Time <- test$Time %>% Convert_Time_Zones(From = attributes(test$Time)$tzone, 
                      To = "America/New_York")
                  if (.y != "VIX") {
                    test <- test %>% dplyr::mutate(Hour = lubridate::hour(Time), 
                      Minute = lubridate::minute(Time))
                    test$Condition <- if (Interval != "60M") 
                      (test$Hour < Late & test$Hour > Early) | 
                        ((test$Hour >= Early & test$Hour < Early + 
                          1) & test$Minute > 29)
                    else (test$Hour < Late & test$Hour > Early) | 
                      ((test$Hour >= Early & test$Hour < Early + 
                        1) & test$Minute >= 0)
                    test <- test[test$Condition, !(colnames(test) == 
                      "Condition")]
                    if (min(as.Date(test$Time)) <= as.Date("2020-12-24")) 
                      test <- test %>% dplyr::filter(!(as.Date(test$Time) %in% 
                        c(as.Date("2020-11-27"), as.Date("2020-12-24")) & 
                        Hour >= 13))
                    test
                  }
                  if (attributes(test$Time)$tzone != "Europe/Ljubljana") 
                    test$Time <- test$Time %>% Convert_Time_Zones(From = attributes(test$Time)$tzone, 
                      To = "Europe/Ljubljana")
                  test
                }
                else if (Continent == "Europe") {
                  test$Time <- test$Time %>% Convert_Time_Zones(From = attributes(test$Time)$tzone, 
                    To = "Europe/Ljubljana")
                  test <- test[format(test$Time, format = "%H:%M:%S") >= 
                    "09:00:00", ]
                  test <- test[format(test$Time, format = "%H:%M:%S") <= 
                    "17:29:00", ]
                  if (min(as.Date(test$Time)) <= as.Date("2020-12-24")) {
                    test <- test %>% dplyr::mutate(Hour = lubridate::hour(Time))
                    test <- test %>% dplyr::filter(!(as.Date(test$Time) %in% 
                      c(as.Date("2020-12-24"), as.Date("2020-12-31")) & 
                      Hour > 7))
                  }
                  test
                }
                else if (Continent == "Asia") {
                  test$Time <- test$Time %>% Convert_Time_Zones(From = attributes(test$Time)$tzone, 
                    To = "Europe/Ljubljana")
                  test <- test[format(test$Time, format = "%H:%M:%S") >= 
                    "01:00:00", ]
                  test <- test[format(test$Time, format = "%H:%M:%S") <= 
                    "07:30:00", ]
                  test
                }
                test
            }
            else {
                test$Time <- test$Time %>% Convert_Time_Zones(From = attributes(test$Time)$tzone, 
                  To = "Europe/Ljubljana")
            }
            test
        }
        else {
            test$Time <- test$Time %>% Convert_Time_Zones(From = attributes(test$Time)$tzone, 
                To = "Europe/Ljubljana")
        }
        test <- test[, c("Time", "Open", "High", "Low", "Close", 
            "Volume")]
        if (all(is.na(test$Volume))) 
            test$Volume <- 0
        test
    })
    Stock_Data <- purrr::imap(Stock_Data, function(x, y) {
        if (y != "VIX") {
            x$Remove <- x$High == x$Low & x$Volume == 0
        }
        else {
            if (!Interval %in% c("1D", "1W", "2W", "1MO")) {
                if (!(Aggregate_By & Interval == "5M")) {
                  x$Remove <- x$High == x$Low & (lubridate::hour(x$Time) > 
                    21 | lubridate::hour(x$Time) < 9)
                }
                else {
                  x$Remove <- x$High == x$Low & ((lubridate::hour(x$Time) > 
                    21 & lubridate::minute(x$Time) == 15) | lubridate::hour(x$Time) < 
                    9)
                }
            }
            else {
                x$Remove <- x$High == x$Low & x$Volume == 0
            }
        }
        x <- x[!x$Remove, colnames(x) != "Remove"]
        x
    })
    if (End_Candle & !Interval %in% c("1D", "1W", "2W", "1MO")) {
        Add <- if (grepl("M", Interval)) 
            gsub("M", "", Interval) %>% as.numeric()
        else 60
        Stock_Data <- lapply(Stock_Data, function(x) {
            x$Time <- x$Time + 60 * Add
            x
        })
    }
    if (Filter_Gaps & !Interval %in% c("1D", "1W", "2W", "1MO")) {
        Allowed_Gap <- if (Interval != "1H") 
            as.numeric(gsub("M", "", Interval)) * 3
        else as.numeric(gsub("H", "", Interval)) * 3 * 60
        Symbols_with_less_than_2_gaps <- names(Stock_Data %>% 
            purrr::map(~.x %>% .$Time %>% diff %>% .[. < Allowed_Gap] %>% 
                unique %>% length) %>% unlist() %>% .[. <= if (!Interval %in% 
            c("60M", "1H")) 2 else 3])
        Stock_Data <- Stock_Data[Symbols_with_less_than_2_gaps]
    }
    if (length(Stock_Data) > 0) {
        if (Only_full_candles) {
            Stock_Data <- if (Interval %in% c("1D", "1W", "2W", 
                "1MO")) {
                Unit <- lubridate::floor_date(if (!lubridate::wday(Sys.Date()) %in% 
                  c(7, 1)) 
                  Sys.Date()
                else Last_Friday() + 3, unit = if (Interval != 
                  "1MO") 
                  Interval %>% gsub("2W", "W", .)
                else "month")
                purrr::map(Stock_Data, function(x) x[as.Date(x$Time) <= 
                  Unit, ])
            }
            else {
                Unit <- if (Aggregate_By) 
                  (if (Interval == "5M") 
                    "15M"
                  else if (Interval == "30M") 
                    "60M")
                else Interval
                Final_Time <- lubridate::floor_date(x = Sys.time(), 
                  unit = Unit)
                if (End_Candle) {
                  purrr::map(Stock_Data, function(x) x[x$Time <= 
                    Final_Time, ])
                }
                else {
                  purrr::map(Stock_Data, function(x) x[x$Time < 
                    Final_Time, ])
                }
            }
        }
        if (Aggregate_By) 
            Stock_Data <- Stock_Data %>% Aggregate_Multiple_Granularities(Aggregate_Size = if (Interval == 
                "5M") 
                3
            else 2, Last_N = 100000, Nrow = 5, End_Candle = End_Candle) %>% 
                .[[ncol(.)]]
        if (Add_Candle_Column) 
            Stock_Data <- purrr::map(Stock_Data, ~.x %>% dplyr::mutate(Candle = ifelse(Aggregate_By, 
                if (Interval == "5M") 
                  "15_Min"
                else if (Interval == "30M") 
                  "60_Min"
                else "2W", gsub("M$", "_Min", Interval) %>% gsub("1D", 
                  "D", .) %>% gsub("1W", "W", .) %>% gsub("1MO", 
                  "M", .) %>% gsub("1H$", "60_Min", .))))
        if (Interval %in% c("1D", "1W", "2W", "1MO")) 
            Stock_Data <- purrr::map(Stock_Data, ~.x %>% dplyr::mutate(Time = as.Date(Time)))
        Stock_Data <- purrr::map(Stock_Data, ~.x %>% data.table::data.table())
        if (Round_to_two_places) {
            Stock_Data <- Stock_Data %>% purrr::map(function(x) {
                Places_to_round <- if (dplyr::last(x$Close) >= 
                  1) 
                  2
                else 4
                for (Col in c("Open", "High", "Low", "Close")) x[[Col]] <- round(x[[Col]], 
                  digits = Places_to_round)
                x
            })
        }
        if (Continent == "Europe") {
            if (Interval %in% c("1W", "2W")) {
                if (".STOXX50E" %in% names(Stock_Data)) {
                  if (last(Stock_Data[[".STOXX50E"]]$Time) != 
                    as.Date(purrr::map(Stock_Data, ~.x %>% .$Time %>% 
                      last) %>% unlist(use.names = F) %>% max(), 
                      origin = "1970-01-01")) {
                    Stock_Data[[".STOXX50E"]]$Time <- Stock_Data[[".STOXX50E"]]$Time + 
                      if (Interval == "1W") 
                        7
                      else if (Interval == "2W") 
                        14
                  }
                }
            }
        }
    }
    Stock_Data
}


Convert_Time_Zones <- function (Times, From = "America/New_York", To = Sys.timezone()) 
{
    Times <- Times %>% lubridate::force_tz(., tzone = From) %>% 
        lubridate::with_tz(., tzone = To)
    Times
}


DF <- function (Dataset) 
{
    Dataset <- data.frame(Dataset)
    Dataset
}


Half_Open_Days <- function () as.Date(c("2023-07-03", "2024-07-03", "2025-07-03", "2023-11-24", "2024-11-29", "2025-11-28", "2024-12-24", "2025-12-24")) %>% sort


Read_last_Business_Day <- function (Country = "US") 
{
    Base <- Base_Folder(Suffix = "", Root = TRUE)
    File <- paste0(Base, "IB_Additional_Data/", if (Country == 
        "EU") 
        "EU_", "Last_Business_Day_", Correct_Colnames(Sys.Date()), 
        ".csv")
    if (!file.exists(File)) {
        list.files(path = paste0(Base, "IB_Additional_Data/"), 
            pattern = paste0(if (Country == "EU") 
                "EU_", "Last_Business_Day"), full.names = T) %>% 
            file.remove
        data.table::fwrite(x = list(Last_Business_Day(Country = if (Country == 
            "EU") "Germany" else "United States")), file = paste0(Base, 
            "IB_Additional_Data/", if (Country == "EU") 
                "EU_", "Last_Business_Day_", Correct_Colnames(Sys.Date()), 
            ".csv"), quote = F, row.names = F)
    }
    else {
        if (Country == "US") {
            if (file.info(File)$mtime < Sys.time() - 1800 | (file.info(File)$mtime < 
                US_Market_Open_and_Close_Hour(Time = "Close") & 
                Sys.time() > US_Market_Open_and_Close_Hour(Time = "Close"))) {
                data.table::fwrite(x = list(Last_Business_Day(Country = "United States")), 
                  file = paste0(Base, "IB_Additional_Data/Last_Business_Day_", 
                    Correct_Colnames(Sys.Date()), ".csv"), quote = F, 
                  row.names = F)
            }
            else if (Country == "EU") {
                EU_Market_Close <- as.POSIXct(paste0(Sys.Date(), 
                  " ", "17:30:00"))
                if (file.info(File)$mtime < Sys.time() - 1800 | 
                  (file.info(File)$mtime < EU_Market_Close & 
                    Sys.time() > EU_Market_Close)) {
                  data.table::fwrite(x = list(Last_Business_Day(Country = "Germany")), 
                    file = paste0(Base, "IB_Additional_Data/EU_Last_Business_Day_", 
                      Correct_Colnames(Sys.Date()), ".csv"), 
                    quote = F, row.names = F)
                }
            }
        }
    }
    Day <- data.table::fread(file = paste0(Base, "IB_Additional_Data/", 
        if (Country == "EU") 
            "EU_", "Last_Business_Day_", Correct_Colnames(Sys.Date()), 
        ".csv")) %>% .[[1]] %>% as.Date()
    Day
}


Time_Diff_Ljubljana_New_York <- function () 
{
    Hour_Diff <- as.numeric(Sys.time() - (lubridate::with_tz(Sys.time(), 
        tzone = "America/New_York") %>% lubridate::force_tz(., 
        tzone = "Europe/Ljubljana"))) %>% round(digits = 0)
    Hour_Diff
}


Investing_Com_Holiday_Calendar <- function () 
{
    webpage <- xml2::read_html("https://www.investing.com/holiday-calendar/")
    Holidays <- webpage %>% rvest::html_nodes("#holidayCalendarData") %>% 
        rvest::html_table(fill = TRUE) %>% .[[1]] %>% .[-1, ]
    for (i in 1:length(Holidays$Date)) {
        if (nchar(Holidays$Date[i]) == 0) {
            Holidays$Date[i] <- Holidays$Date[i - 1]
        }
    }
    Holidays <- Holidays %>% dplyr::mutate(Date = lubridate::mdy(Date))
    return(Holidays)
}


Curl_Multi_Json_Data <- function (URL_Prefix, Variable, URL_Suffix, FUN = NA, Names = TRUE, 
    Match_URLs = T, N = 100, Verbose = F, ...) 
{
    Start <- Sys.time()
    pool <- curl::new_pool()
    uris <- paste0(URL_Prefix, Variable, URL_Suffix)
    if (!Match_URLs) 
        uris <- rep(uris, N)
    data <- vector("list", length = length(uris))
    if (Match_URLs) {
        cb <- function(req, Match_URLs = Match_URLs) {
            Match <- match(req$url, uris)
            if (!is.na(Match)) 
                data[[Match]] <<- rawToChar(req$content)
        }
    }
    else {
        cb <- function(req, Match_URLs = Match_URLs) {
            data[[which(sapply(data, is.null))[1]]] <<- rawToChar(req$content)
        }
    }
    sapply(uris, curl::curl_fetch_multi, done = cb, pool = pool, 
        ...)
    out <- curl::multi_run(pool = pool)
    if (Names) 
        names(data) <- Variable
    data <- data %>% purrr::compact()
    Index <- which(sapply(data, jsonlite::validate))
    data <- data[Index] %>% purrr::map(., ~jsonlite::fromJSON(.x))
    if (!is.na(FUN)) {
        Fun_To_Apply <- match.fun(FUN)
        data <- purrr::map(data, function(JSON) Fun_To_Apply(JSON, 
            ...))
    }
    data <- data %>% purrr::compact()
    if (Verbose) 
        print(Sys.time() - Start)
    data
}


Last_Friday <- function () Sys.Date() - lubridate::wday(Sys.Date() + 1)


Numeric_to_Date <- function (Numeric_Date, Divisor = 1, TZ = "UTC") 
{
    Dates <- as.POSIXct(as.numeric(Numeric_Date)/Divisor, origin = "1970-01-01", 
        tz = TZ)
    Dates
}


Last_Business_Day <- function (Date = Sys.Date(), Write = F, Country = "United States") 
{
    Holiday <- Market_Holidays(Country = Country, Date = Date)
    Last_Buz_Days <- Business_Days(From = Sys.Date() - 7, To = Sys.Date(), 
        Country = if (Country == "United States") 
            "US"
        else "EU")
    if (!Holiday) {
        if (Country == "United States") {
            Last <- if (Sys.time() < US_Market_Open_and_Close_Hour() %>% 
                .$Close) {
                Last_Buz_Days %>% Filter(f = function(x) x < 
                  Sys.Date(), x = .) %>% max()
            }
            else if (Sys.time() > US_Market_Open_and_Close_Hour() %>% 
                .$Close) {
                Last_Buz_Days %>% max()
            }
        }
        else {
            EU_Market_Close <- as.POSIXct(paste0(Sys.Date(), 
                " ", "17:30:00"))
            Last <- if (Sys.time() < EU_Market_Close) {
                Last_Buz_Days %>% Filter(f = function(x) x < 
                  Sys.Date(), x = .) %>% max()
            }
            else if (Sys.time() > EU_Market_Close) {
                Last_Buz_Days %>% max()
            }
        }
    }
    else {
        Last <- Last_Buz_Days %>% Filter(f = function(x) x < 
            Sys.Date(), x = .) %>% max()
    }
    if (Write) {
        File <- paste0(Base_Folder(Suffix = "", Root = TRUE), 
            "IB_Additional_Data/", if (Country %in% c("Germany", 
                "EU")) 
                "EU_", "Last_Business_Day_", Correct_Colnames(Sys.Date()), 
            ".csv")
        if (file.exists(File)) 
            file.remove(File)
        data.table::fwrite(x = list(Last), file = , quote = F, 
            row.names = F)
    }
    Last
}


Business_Days <- function (From, To = Sys.Date(), Country = "US") 
{
    if (To < From) 
        To <- From
    To <- as.Date(To)
    From <- as.Date(From)
    Min_Year <- lubridate::year(From)
    Max_Year <- lubridate::year(To)
    Holidays <- lapply(Min_Year:Max_Year, function(Year) {
        if (Country == "US") 
            timeDate:::holidayNYSE(year = Year)
        else timeDate:::holidayLONDON(year = Year)
    } %>% as.Date) %>% unlist(use.names = F) %>% as.Date(origin = "1970-01-01") %>% 
        c(., "2022-06-20") %>% sort
    if (From != To) {
        Biz_Days <- seq(From, To, by = 1)[!(seq(From, To, by = 1) %in% 
            Holidays)] %>% as.Date(origin = "1970-01-01")
        Biz_Days <- Biz_Days[timeDate::isWeekday(Biz_Days)]
    }
    else {
        Biz_Days <- From[timeDate::isWeekday(From)]
    }
    Biz_Days
}


Filter_YF_Data <- function (Dataset, Smooth = F, Last_N = 60, End_Candle = End_Candle) 
{
    Market_Open <- US_Market_Open()
    if (class(Dataset)[1] == "list") {
        EU <- any(grepl(names(Dataset), pattern = "-GB|-NL|-DK|-GR|-PT|-BE|-FI|-DE|-FR|-IT|-SE|\\.GB|\\.NL|\\.DK|\\.GR|\\.PT|\\.BE|\\.FI|\\.DE|\\.FR|\\.IT|\\.SE")) | 
            any(startsWith(x = names(Dataset), prefix = "."))
        if (length(Dataset) > 0) {
            Dataset <- purrr::imap(Dataset, function(x, y) {
                x <- tryCatch(expr = if (is.na(x[nrow(x) - 1, 
                  "Close"])) 
                  head(x, -2)
                else if (is.na(x[nrow(x), "Close"])) 
                  head(x, -1)
                else x, error = function(e) NULL)
            }) %>% purrr::compact()
            Dataset <- if (!is.null(Last_N)) 
                Dataset %>% purrr::keep(~!(.x %>% .$Close %>% 
                  tail(Last_N) %>% is.na %>% any)) %>% purrr::map(~.x %>% 
                  tail(Last_N))
            else Dataset
            Dataset <- Dataset %>% purrr::keep(~.x %>% nrow > 
                0)
            if (length(Dataset) > 0) {
                Max_Time <- if (length(grep("VIX", names(Dataset))) > 
                  0) {
                  if (length(Dataset[-grep("VIX", names(Dataset))]) > 
                    0) {
                    purrr::map(Dataset[-grep("VIX", names(Dataset))], 
                      function(x) dplyr::last(x$Time)) %>% unlist(use.names = F) %>% 
                      max()
                  }
                  else {
                    purrr::map(Dataset, function(x) dplyr::last(x$Time)) %>% 
                      unlist(use.names = F) %>% max()
                  }
                }
                else {
                  purrr::map(Dataset, function(x) dplyr::last(x$Time)) %>% 
                    unlist(use.names = F) %>% max()
                }
                if (all(lubridate::second(Numeric_to_Date(Max_Time)) == 
                  0 & class(Dataset[[1]]$Time) != "Date")) {
                  Dataset <- lapply(Dataset, function(x) {
                    x <- x[x$Time <= Max_Time, ]
                    x
                  })
                  Dataset <- Dataset %>% purrr::keep(~.x %>% 
                    .$Time %>% last == Max_Time)
                }
                if (Smooth) 
                  Dataset <- Dataset %>% purrr::map(~.x %>% Smooth_NA_Values)
            }
            Dataset
        }
        Dataset
    }
    else if (any(c("data.frame", "tbl_df") %in% class(Dataset))) {
        if ("Time" %in% attributes(Dataset)$names) {
            if (!"Symbol" %in% dplyr::group_vars(Dataset)) 
                Dataset <- Dataset %>% dplyr::group_by(Symbol)
            Dataset <- Dataset %>% dplyr::add_tally() %>% dplyr::filter(n >= 
                Last_N) %>% dplyr::select(-n)
            Remove_Tail <- function(x, ...) if (is.na(x[nrow(x) - 
                1, "Close"])) 
                head(x, -2L)
            else head(x, -1L)
            Dataset <- if (Market_Open) 
                Dataset %>% dplyr::group_modify(Remove_Tail) %>% 
                  dplyr::filter(!is.na(last(Close)))
            else Dataset
            Dataset <- if (!is.null(Last_N)) 
                Dataset %>% dplyr::filter(!any(is.na(tail(Close, 
                  n = Last_N)))) %>% dplyr::group_modify(~tail(.x, 
                  Last_N))
            else Dataset
            Dataset <- if (Smooth) 
                Dataset %>% dplyr::do(Smooth_NA_Values(.))
            else Dataset
            Max_Time <- Dataset %>% dplyr::group_modify(~tail(.x, 
                1)) %>% .$Time %>% max()
            if (lubridate::second(Numeric_to_Date(Max_Time)) == 
                0 & class(Dataset$Time) != "Date") 
                Dataset <- Dataset %>% dplyr::filter(dplyr::last(Time) == 
                  Max_Time)
            Dataset <- Dataset %>% dplyr::ungroup()
        }
        else {
            Dataset <- if (Market_Open) {
                purrr::map(Dataset %>% .[-1], function(Column) purrr::map(Column, 
                  function(x) head(x, -2)) %>% set_names(Dataset[[1]]) %>% 
                  Imap_and_rbind() %>% dplyr::group_by(Symbol) %>% 
                  dplyr::filter(!is.na(last(Close))) %>% {
                  if (!is.null(Last_N)) 
                    dplyr::filter(., !any(is.na(tail(Close, n = Last_N))))
                  else .
                } %>% {
                  if (Smooth) 
                    Smooth_NA_Values(.)
                  else .
                } %>% dplyr::ungroup()) %>% purrr::imap(~cbind(.x, 
                  Candle = .y)) %>% dplyr::bind_rows() %>% tidyr::nest(-Symbol, 
                  -Candle) %>% tidyr::spread(., Candle, -Symbol) %>% 
                  .[, colnames(Dataset)]
            }
            else {
                Dataset
            }
        }
    }
    Dataset
}


Imap_and_rbind <- function (Named_List, New_Column_Name = "Symbol") 
{
    if (length(Named_List) > 0) {
        if (New_Column_Name %in% colnames(Named_List[[1]])) {
            Named_List <- Named_List %>% dplyr::bind_rows()
        }
        else {
            Named_List <- Named_List %>% purrr::imap(~cbind(.x, 
                bla = .y)) %>% dplyr::bind_rows() %>% dplyr::rename(`:=`(!!as.name(New_Column_Name), 
                bla))
        }
    }
    Named_List
}


