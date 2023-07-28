import scrapy
import pandas as pd
import time

class OilPriceSpider(scrapy.Spider):
    name = "oil_prices"
    start_urls = ["https://oilprice.com/oil-price-charts/#prices"]

    def parse(self, response):
        table = response.xpath("//table[@id='prices']")
        headers = table.xpath(".//th//text()").getall()
        rows = []
        for row in table.xpath(".//tr[position()>1]"):
            data = row.xpath(".//td//text()").getall()
            rows.append(data)

        df = pd.DataFrame(rows, columns=headers)
        df['Last'] = df['Last'].str.replace(',', '').astype(float)  # Convert 'Last' column to numeric
        self.update_excel_with_changed_values(df)

    def update_excel_with_changed_values(self, df):
        file_name = "oil_prices.xlsx"
        try:
            prev_data = pd.read_excel(file_name, engine='openpyxl')
            prev_data = prev_data.drop(columns="Unnamed: 0", errors="ignore")
        except FileNotFoundError:
            prev_data = pd.DataFrame()

        if not prev_data.empty:
            last_row = prev_data.iloc[-1]
            df_changed = df.loc[(df['Last'] != last_row['Last']) |
                                (df['Change'] != last_row['Change']) |
                                (df['% Change'] != last_row['% Change']) |
                                (df['Last Updated'] != last_row['Last Updated'])]
        else:
            df_changed = df

        if not df_changed.empty:
            df = pd.concat([prev_data, df_changed], ignore_index=True)
            print("Updated DataFrame:")
            print(df)

        df.to_excel(file_name, index=False, engine='openpyxl')

if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess

    # Initialize the Scrapy settings
    settings = {
        "BOT_NAME": "oil_prices_bot",
        "ROBOTSTXT_OBEY": True,
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}

    process = CrawlerProcess(settings)
    process.crawl(OilPriceSpider)
    process.start()

    # Run the script for 5 minutes, updating the Excel file every 20 seconds.
    end_time = time.time() + 300  # 300 seconds = 5 minutes
    while time.time() < end_time:
        time.sleep(20)
        process.crawl(OilPriceSpider)
        process.start()
