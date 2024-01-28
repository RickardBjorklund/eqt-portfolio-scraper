import asyncio
import aiohttp
from tldextract import tldextract


async def fetch_companies(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            try:
                r = await response.json()
                return r["result"]["data"]["allSanityCompanyPage"]["nodes"]
            except Exception as err:
                print(f"Unexpected {err=}, {type(err)=}")
                raise


async def fetch_company_details(session, path):
    async with session.get(f"https://eqtgroup.com/page-data{path}page-data.json") as response:
        try:
            r = await response.json()
            return r["result"]["data"]["sanityCompanyPage"]
        except:
            return None


async def get_registered_domain(url):
    if not isinstance(url, str):
        return None
    return tldextract.extract(url).registered_domain


async def get_logo_url(logo):
    if logo is None:
        return None
    if logo.get("asset") is None:
        return None
    return logo.get("asset").get("url")


async def get_description(raw_body):
    if not raw_body:
        return None
    description = ""
    for part in raw_body:
        for sub_part in part.get("children", []):
            description += sub_part.get("text", "")  # TODO: Maybe add space or newline after each sub_part
    return description


async def extract_details(company_details):
    interesting_details = {
        "description": await get_description(company_details.get("_rawBody", [])),
        "preamble": company_details.get("preamble"),
        "heading": company_details.get("heading"),
        "responsible_advisors": [advisor.get("title") for advisor in company_details.get("responsibleAdvisors", [])],
        "website": company_details.get("website"),
        "registered_domain": await get_registered_domain(company_details.get("website")),
        "board": company_details.get("board", []),
        "management": company_details.get("management", []),
        "logo": await get_logo_url(company_details.get("logo")),
    }

    # This data exists on the details page, but also in the company listing
    # redundant_details = {
    #     "title": company_details.get("title", ""),
    #     "country": company_details.get("country", ""),
    #     "entry": company_details.get("entryDate", ""),
    #     "exit": company_details.get("exitDate", ""),
    #     "sector": company_details.get("sector", ""),
    #     "fund": [fund.get("title", "") for fund in company_details.get("fund", [])],
    # }

    return interesting_details


async def get_company_data_singular(session, company):
    company_data = {
        "title": company.get("title"),
        "sector": company.get("sector"),
        "country": company.get("country"),
        "fund": [fund.get("title", "") for fund in company.get("fund", [])],
        "entry": company.get("entryDate"),
        "exit": company.get("exitDate"),
        "company_details_path": company.get("path"),
    }

    # promotedSdg, sdg and topic exist in the page data,
    # but are always (with one exception where promotedSdg=3) undefined.
    # The id doesn't really seem useful in this context as it is not correlating to the id in the data from gcs.
    # unused_company_data = {
    #     "promotedSdg": company.get("promotedSdg"),
    #     "sdg": company.get("sdg"),
    #     "topic": company.get("topic"),
    #     "_id": company.get("_id"),
    # }

    company_details = await fetch_company_details(session, company.get("path"))
    if not company_details:
        print(f"No details could be found for: {company.get("title")}")
        return company_data
    else:
        interesting_details = await extract_details(company_details)
        return company_data | interesting_details


async def get_company_data(companies):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for company in companies:
            task = asyncio.create_task(get_company_data_singular(session, company))
            tasks.append(task)
        company_data = await asyncio.gather(*tasks)
    return company_data
