from unittest import TestCase

from playwright.sync_api import sync_playwright

from utils import write_json_file


def scrape_sub_page(browser, url):
    page = browser.new_page(viewport={"width": 1920, "height": 1080})
    page.goto(f"https://eqtgroup.com{url}")

    # Wait for page to load
    page.wait_for_load_state("networkidle")
    page.wait_for_load_state("domcontentloaded")

    left_column = page.query_selector("//*[@id='gatsby-focus-wrapper']/main/div[1]")

    heading = left_column.query_selector("h2.font-h3")
    preamble = left_column.query_selector("p.font-preamble")

    result = {
        "heading": heading.text_content() if heading else None,
        "preamble": preamble.text_content() if preamble else None,
    }

    tabular_information = left_column.query_selector_all("li.flex")
    for item in tabular_information:
        parts = item.query_selector_all("div.flex-1")
        if len(parts) > 1:
            key = parts[0].text_content().lower().replace(" ", "_")
            value = parts[1].text_content()
            if key in result:
                existing_value = result[key]
                if not isinstance(existing_value, list):
                    # If the existing value is not a list, convert it to a list
                    result[key] = [existing_value]
                result[key].append(value)
            else:
                # If the key doesn't exist, add it with the value
                result[key] = value

    description_text = ""
    description = page.query_selector("//*[@id='gatsby-focus-wrapper']/main/div[4]")
    if description:
        description_parts = description.query_selector_all("p.font-body")
        for part in description_parts:
            description_text += part.text_content()
    result["description"] = description_text

    page.close()
    return result


def scrape_website(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1920, "height": 1080})
        page.goto(url)

        # Wait for page to load
        page.wait_for_load_state("networkidle")
        page.wait_for_load_state("domcontentloaded")

        company_table = page.query_selector("//*[@id='content']/div[2]/div/div/div[2]/ul")
        company_table_rows = company_table.query_selector_all("li.items-center")

        data = []

        for row in company_table_rows:
            # Ensure there are at least 4 spans as expected
            parts = row.query_selector_all("span.flex-1")
            if len(parts) > 4:
                title = parts[0].text_content()
                sector = parts[1].text_content()
                country = parts[2].text_content()
                fund = parts[3].text_content()
                portfolio_entry = parts[4].text_content()
                try:
                    portfolio_exit = parts[5].text_content()
                except IndexError:
                    portfolio_exit = None

                # Cleaning up and structuring the data
                company_data = {
                    "title": title.strip(),
                    "sector": sector.strip(),
                    "country": country.strip(),
                    "fund": fund.strip(),
                    "entry": portfolio_entry.strip(),
                    "exit": portfolio_exit.strip() if portfolio_exit else None,
                }

                link_element = parts[0].query_selector("a")
                page_link = link_element.get_attribute("href") if link_element else None
                if page_link:
                    company_details = scrape_sub_page(browser, page_link)
                    company_data |= company_details

                data.append(company_data)

        browser.close()
        return data


class TestScraping(TestCase):

    def test_scraping(self):
        data = scrape_website("https://eqtgroup.com/current-portfolio/")
        print("Success", len(data))
        write_json_file("current_portfolio_scraped_with_playwright.json", data)

    def test_subpage_scraping(self):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            data = scrape_sub_page(browser, "/current-portfolio/3shape/")
            print("Success", len(data))
            write_json_file("3shape_scraped_with_playwright.json", data)
