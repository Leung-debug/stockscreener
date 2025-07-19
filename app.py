import subprocess
import sys
import os
import time

# Import the necessary libraries
import streamlit as st
import requests
from tradingview_screener import Query, col
import pandas as pd
import time
import yfinance as yf
from yfinance import Search
import numpy as np
from datetime import datetime
import pytz
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import JsCode


eastern = pytz.timezone("US/Eastern")
now_us = datetime.now(eastern)
# Define time thresholds
market_open = now_us.replace(hour=9, minute=30, second=0, microsecond=0)
mid_morning = now_us.replace(hour=10, minute=30, second=0, microsecond=0)
early_afternoon = now_us.replace(hour=12, minute=30, second=0, microsecond=0)

# Determine volume threshold
if market_open <= now_us < mid_morning:
    min_volume = 5000000
elif mid_morning <= now_us < early_afternoon:
    min_volume = 15000000
elif now_us >= early_afternoon:
    min_volume = 20000000
else:
    min_volume = 500000  # before market open

print(f"⏰ Current US Time: {now_us.strftime('%I:%M %p')} | Volume Threshold: {min_volume:,}")


def fetchPremarketData():
    _, preMarketGainers = (Query()
                        .select('name', 'premarket_change','premarket_close', 'premarket_volume', 'premarket_gap', 'close','volume')
                        .where(
                            col('premarket_close')  <  8,
                            col('premarket_volume')>= 500000,
                            col('exchange').isin(['AMEX', 'CBOE', 'NASDAQ', 'NYSE']),
                            col('is_primary') == True,
                            col('typespecs').has('common'),
                            col('typespecs').has_none_of('preferred'),
                            col('type') == 'stock',
                            col('premarket_change') > 10,
                            col('premarket_change').not_empty(),
                            col('active_symbol') == True,
                        )
                        .order_by('premarket_change', ascending=False, nulls_first=False)
                        .limit(130)
                        .set_markets('america')
                        .set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr', 'structured']}})
                        .set_property('preset', 'pre-market-gainers')
                        .get_scanner_data())


    # if you want a nicer column name for the symbol:
    preMarketGainers = preMarketGainers.rename(columns={'name':'Symbol',
                                                        'premarket_change':'PreMarket % Change',
                                                        'premarket_close':'PreMarket Price',
                                                        'premarket_volume':'PreMarket Volume',
                                                        'premarket_gap':'PreMarket Gap %',
                                                        'close':'Previous Day Close Price',
                                                        'volume':'Previous Day Volume',
                                                        })
    preMarketGainers = preMarketGainers.drop(columns=['ticker'], errors='ignore')
    # print(preMarketGainers.columns.tolist())
    # print(preMarketGainers)

    symbols = preMarketGainers['Symbol'].tolist()
    enriched_data = []

    for symbol in symbols:
        data = {'Symbol': symbol}

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            # Basic info
            data["52W Low"] = info.get("fiftyTwoWeekLow", np.nan)
            data["52W High"] = info.get("fiftyTwoWeekHigh", np.nan)
            data["Target Mean Price"] = info.get("targetMeanPrice", np.nan)
            data["Target Low Price"] = info.get("targetLowPrice", np.nan)
            data["Target High Price"] = info.get("targetHighPrice", np.nan)
            data["Description"] = info.get("longBusinessSummary", np.nan)
            # data["Market Cap"] = info.get("marketCap", np.nan)
            # data["Sector"] = info.get("sector", np.nan)
            # data["# Analyst Opinions"] = info.get("numberOfAnalystOpinions", np.nan)


            # Historical ranges
            try:
                hist = ticker.history(period="30d", interval="1d")

                if not hist.empty and len(hist) >= 2:
                    prev_day = hist.iloc[-2]
                    data["1Day Range"] = f"${prev_day['Low']:.2f} – ${prev_day['High']:.2f}"

                    last_1w = hist.tail(5)
                    data["1W Range"] = f"${last_1w['Low'].min():.2f} – ${last_1w['High'].max():.2f}"

                    last_1m = hist.tail(22)
                    data["1M Range"] = f"${last_1m['Low'].min():.2f} – ${last_1m['High'].max():.2f}"
                else:
                    data["1Day Range"] = np.nan
                    data["1W Range"] = np.nan
                    data["1M Range"] = np.nan

            except Exception as e:
                print(f"⚠️ Range error for {symbol}: {e}")
                data["1Day Range"] = data["1W Range"] = data["1M Range"] = np.nan


            # News
            try:
                search = Search(symbol)
                articles = search.news
                top_news = []
                for article in articles[:3]:
                    title = article.get("title")
                    link = article.get("link")
                    if title and link:
                        top_news.append(f"- {title} ({link})")
                data["Recent News"] = "\n".join(top_news) if top_news else np.nan
            except Exception as e:
                print(f"⚠️ News fetch failed for {symbol}: {e}")
                data["Recent News"] = np.nan

        except Exception as e:
            print(f"⚠️ Failed to fetch data for {symbol}: {e}")
            data.update({
                "52W Low": np.nan,
                "52W High": np.nan,
                "Target Mean Price": np.nan,
                "Target Low Price": np.nan,
                "Target High Price": np.nan,
                "Description": np.nan,
                "1Day Range": np.nan,
                "1W Range": np.nan,
                "1M Range": np.nan,
                "Recent News": np.nan
            })

        enriched_data.append(data)

    yahoo_enriched_df = pd.DataFrame(enriched_data)

    final_df = preMarketGainers.merge(yahoo_enriched_df, how='left', left_on='Symbol', right_on='Symbol')
    final_df = final_df.sort_values(by='PreMarket % Change', ascending=False)
    
    return final_df



def fetchMarketData():
    _, marketGainers = (Query()
                            .select('name','close', 'change','volume',)
                            .where(
                                col('close')  <  8,
                                col('change') >= 15,
                                col('volume')>= min_volume,
                                col('exchange').isin(['AMEX', 'CBOE', 'NASDAQ', 'NYSE']),
                                col('is_primary') == True,
                                col('typespecs').has('common'),
                                col('typespecs').has_none_of('preferred'),
                                col('type') == 'stock',
                                col('active_symbol') == True,
                            )
                            .order_by('Volatility.D', ascending=False, nulls_first=False)
                            .limit(100)
                            .set_markets('america')
                            .set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr', 'structured']}})
                            .set_property('preset', 'most_volatile')
                            .get_scanner_data())

    # if you want a nicer column name for the symbol:
    marketGainers = marketGainers.rename(columns={'name':'Symbol',
                                                'change':'% Change',
                                                'close':'Price',
                                                'volume':'Volume',
                                                        })

    marketGainers = marketGainers.drop(columns=['ticker'], errors='ignore')
    # print(marketGainers.columns.tolist())
    # print(marketGainers)



    _, activeMarketGainers = (Query()
                            .select('name')
                            .where(
                                col('exchange').isin(['AMEX', 'CBOE', 'NASDAQ', 'NYSE']),
                                col('is_primary') == True,
                                col('typespecs').has('common'),
                                col('typespecs').has_none_of('preferred'),
                                col('type') == 'stock',
                                col('close').between(2, 10000),
                                col('active_symbol') == True,
                            )
                            .order_by('Value.Traded', ascending=False, nulls_first=False)
                            .limit(100)
                            .set_markets('america')
                            .set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr', 'structured']}})
                            .set_property('preset', 'volume_leaders')
                            .get_scanner_data())


    activeMarketGainers = activeMarketGainers.rename(columns={'name': 'Symbol'})

    # Step 1: Convert active names into a Python set for fast lookup
    active_symbols = set(activeMarketGainers['Symbol'])

    # Step 2: Create a new "Status" column in marketGainers
    marketGainers['Status'] = [
        "ACTIVE" if symbol in active_symbols else "N/A"
        for symbol in marketGainers['Symbol']
    ]


    md_symbols = marketGainers['Symbol'].tolist()
    enriched_marketData = []

    for symbol in md_symbols:
        marketData = {'Symbol': symbol}

        try:
            md_ticker = yf.Ticker(symbol)
            md_info = md_ticker.info
            # Basic info
            marketData["52W Low"] = md_info.get("fiftyTwoWeekLow", "N/A")
            marketData["52W High"] = md_info.get("fiftyTwoWeekHigh", "N/A")
            marketData["Target Mean Price"] = md_info.get("targetMeanPrice", np.nan)
            marketData["Target Low Price"] = md_info.get("targetLowPrice", np.nan)
            marketData["Target High Price"] = md_info.get("targetHighPrice", np.nan)
            marketData["Description"] = md_info.get("longBusinessSummary", "N/A")
            # marketData["Market Cap"] = md_info.get("marketCap", "N/A")
            # marketData["Sector"] = md_info.get("sector", "N/A")
            # marketData["# Analyst Opinions"] = md_info.get("numberOfAnalystOpinions", "N/A")


            # Historical ranges
            try:
                md_hist = md_ticker.history(period="30d", interval="1d")

                if not md_hist.empty and len(md_hist) >= 2:
                    md_prev_day = md_hist.iloc[-2]
                    marketData["1Day Range"] = f"${md_prev_day['Low']:.2f} – ${md_prev_day['High']:.2f}"

                    md_last_1w = md_hist.tail(5)
                    marketData["1W Range"] = f"${md_last_1w['Low'].min():.2f} – ${md_last_1w['High'].max():.2f}"

                    md_last_1m = md_hist.tail(22)
                    marketData["1M Range"] = f"${md_last_1m['Low'].min():.2f} – ${md_last_1m['High'].max():.2f}"
                else:
                    marketData["1Day Range"] = "N/A"
                    marketData["1W Range"] = "N/A"
                    marketData["1M Range"] = "N/A"

            except Exception as e:
                print(f"⚠️ Range error for {symbol}: {e}")
                marketData["1Day Range"] = marketData["1W Range"] = marketData["1M Range"] = "N/A"


            # News
            try:
                md_search = Search(symbol)
                md_articles = md_search.news
                md_top_news = []
                for article in md_articles[:3]:
                    title = article.get("title")
                    link = article.get("link")
                    if title and link:
                        md_top_news.append(f"- {title} ({link})")
                marketData["Recent News"] = "\n".join(md_top_news) if md_top_news else "N/A"
            except Exception as e:
                print(f"⚠️ News fetch failed for {symbol}: {e}")
                marketData["Recent News"] = "N/A"

        except Exception as e:
            print(f"⚠️ Failed to fetch marketData for {symbol}: {e}")
            marketData.update({
                "52W Low": "N/A",
                "52W High": "N/A",
                "Target Mean Price": "N/A",
                "Target Low Price": "N/A",
                "Target High Price": "N/A",
                "Description": "N/A",
                "1Day Range": "N/A",
                "1W Range": "N/A",
                "1M Range": "N/A",
                "Recent News": "N/A"
            })

        enriched_marketData.append(marketData)

    yahoo_enriched_marketDF = pd.DataFrame(enriched_marketData)
    final_marketDF = marketGainers.merge(yahoo_enriched_marketDF, how='left', left_on='Symbol', right_on='Symbol')
    final_marketDF = final_marketDF.sort_values(by='% Change', ascending=False)
    
    return final_marketDF

def format_number(num):
    try:
        num = float(num)
        if num >= 1_000_000:
            return f"{num / 1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num / 1_000:.0f}K"
        else:
            return f"{num:.0f}"
    except:
        return num

def format_percentage(val):
    try:
        return f"{float(val):.2f}"
    except:
        return val
    


# Streamlit App UI
def main():
    st.markdown("""
    ---
    🔒 **Disclaimer**  
    The information presented in this app is for **informational purposes only** and **does not constitute financial advice, investment recommendations, or an offer to buy or sell any stocks or securities**.  
    Please conduct your own research and consult with a licensed financial advisor before making any investment decisions. The app is intended to display publicly available market data and does **not endorse or promote any specific stock**. Users assume full responsibility for any actions taken based on the information displayed.
    """)

    st.set_page_config(page_title="Stock Screener", layout="wide")

    st.title('📈 Stock Screener Dashboard')
    centered_style = {'textAlign': 'center'}

    light_blue_style = {
        "textAlign": "center",
        "backgroundColor": "#1f4e79",  # light blue hex code
        "fontSize": "13px"
    }

    dark_purple_style = {
        "textAlign": "center",
        "backgroundColor": "#183D3D"  # dark purple hex code (Indigo)
    }

    st.markdown("Pre-Market Gainers (under $8 with >10% gain and 500k Volume)")
    premarket_df = fetchPremarketData()

    premarket_df['PreMarket % Change'] = premarket_df['PreMarket % Change'].apply(format_percentage)
    premarket_df['PreMarket Gap %'] = premarket_df['PreMarket Gap %'].apply(format_percentage)
    premarket_df['PreMarket Volume'] = premarket_df['PreMarket Volume'].apply(format_number)
    premarket_df['Previous Day Volume'] = premarket_df['Previous Day Volume'].apply(format_number)

    gb = GridOptionsBuilder.from_dataframe(premarket_df)
    gb.configure_default_column(resizable=True, filter=True, sortable=True, wrapText=True)
    for col in premarket_df.columns:
        gb.configure_column(col, cellStyle=centered_style)

    gb.configure_column("Symbol", width=100, cellStyle=light_blue_style)
    gb.configure_column("PreMarket % Change", width=150, cellStyle=dark_purple_style)
    gb.configure_column("PreMarket Price", width=130)
    gb.configure_column("PreMarket Volume", width=150, cellStyle=dark_purple_style)

    grid_options = gb.build()

    AgGrid(premarket_df, gridOptions=grid_options, height=400, use_container_width=True)

    st.caption(f"Last updated: {pd.Timestamp.now(tz='US/Eastern').strftime('%Y-%m-%d %H:%M:%S')} EST")
    st.download_button("Download Pre-Market CSV", premarket_df.to_csv(index=False).encode('utf-8-sig'), "premarket_gainers.csv", "text/csv")




    st.markdown("Market Gainers (Volatile, >15% gain)")
    market_df = fetchMarketData()

    market_df['% Change'] = market_df['% Change'].apply(format_percentage)
    market_df['Volume'] = market_df['Volume'].apply(format_number)


    gb = GridOptionsBuilder.from_dataframe(market_df)
    gb.configure_default_column(resizable=True, filter=True, sortable=True, wrapText=True)
    for col in market_df.columns:
        gb.configure_column(col, cellStyle=centered_style)

    gb.configure_column("Symbol", width=100, cellStyle=light_blue_style)
    gb.configure_column("% Change", width=150, cellStyle=dark_purple_style)
    gb.configure_column("Volume", width=150, cellStyle=dark_purple_style)
    gb.configure_column("Description", maxWidth=100)
    grid_options = gb.build()

    AgGrid(market_df, gridOptions=grid_options, height=400, use_container_width=True)

    st.caption(f"Last updated: {pd.Timestamp.now(tz='US/Eastern').strftime('%Y-%m-%d %H:%M:%S')} EST")
    st.download_button("Download Market CSV", market_df.to_csv(index=False).encode('utf-8-sig'), "market_gainers.csv", "text/csv")


if __name__ == '__main__':
    main()

