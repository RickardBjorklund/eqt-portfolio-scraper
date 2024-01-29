import multiprocessing as mp

from playwright.sync_api import sync_playwright


def scrape_sub_page(company):
    url = company.get("company_details_path")
    if url is None:
        return {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
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
        browser.close()

        return result


def scrape_company_listings(url):
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
                link_element = parts[0].query_selector("a")
                page_link = link_element.get_attribute("href") if link_element else None

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
                data.append({
                    "title": title.strip(),
                    "sector": sector.strip(),
                    "country": country.strip(),
                    "fund": fund.strip(),
                    "entry": portfolio_entry.strip(),
                    "exit": portfolio_exit.strip() if portfolio_exit else None,
                    "company_details_path": page_link
                })

        browser.close()
        return data


def scrape_website(url):
    companies = scrape_company_listings(url)
    with mp.Pool(processes=mp.cpu_count()) as pool:
        company_details = pool.map(scrape_sub_page, companies)

    if len(companies) != len(company_details):
        print(f"Companies length ({len(companies)}) did not match length of company details ({len(company_details)})")
        return companies

    for index, company in enumerate(companies):
        company |= company_details[index]

    return companies
