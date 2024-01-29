import unittest
from unittest import mock

from src.page_data.page_data_sync import extract_details, get_description, get_company_data
from src.utils import read_json_file


def mocked_fetch_company_details_no_details(*args, **kwargs):
    return None


def mocked_fetch_company_details(*args, **kwargs):
    return {"company_details": "mocked"}


def mocked_extract_details(*args, **kwargs):
    return {"extracted_details": "mocked"}


class TestPageData(unittest.TestCase):
    def test_get_description(self):
        expected_results = "About\nHeadquartered in Cincinnati, Ohio, First Student and First Transit are dedicated to providing safe, reliable and cost-effective transportation services to school districts, cities, enterprises and their constituents. First Student is the clear market leader in student transportation and focuses on providing contracted services for home-to-school transportation, including for special education, homeless and other student populations. First Transit operates a comprehensive portfolio of complementary public transportation services on behalf of cities, municipalities, and businesses, including fixed route buses and trains, paratransit services, shuttle buses, outsourced vehicle maintenance and other services.Market trends and drivers\nStudent and public transportation are critical components of the US and Canadian educational and economic ecosystems, with student success metrics and funding directly correlated to student attendance, and millions of individuals across the socioeconomic spectrum relying on public transportation daily to get to work, access healthcare, and contribute to the broader economy. As a result of the essential nature of services provided, the public transportation end market is highly stable through economic cycle and benefits from favourable tailwinds including increasing demand for outsourcing and for safer, smarter and more environmentally friendly transportation options. The growing capital intensity and operational complexity associated with digitizing operations and electrifying the fleet will place greater demand across the value chain and favor large, resource-rich outsourced providers, such as First Student and First Transit, who bring best-in-class safety, reliability, capital, and technological expertise to districts and governments.Investment potential\nEQT Infrastructure is committed to building upon the success First Student and First Transit have achieved by making investments in organizational, operational, digital, and sustainability initiatives to further improve and differentiate the Company’s service offering. Most notably, EQT Infrastructure intends to help future-proof the Company by investing in the electrification of its fleet and accelerating its transition to renewable fuel sources in order to support passenger health and reduce environmental impact."
        cd = read_json_file("tests/sample_data/company_details.json")
        description = get_description(cd["_rawBody"])
        self.assertEqual(description, expected_results)

    def test_extract_details(self):
        expected_results = {
            "description": "About\nHeadquartered in Cincinnati, Ohio, First Student and First Transit are dedicated to providing safe, reliable and cost-effective transportation services to school districts, cities, enterprises and their constituents. First Student is the clear market leader in student transportation and focuses on providing contracted services for home-to-school transportation, including for special education, homeless and other student populations. First Transit operates a comprehensive portfolio of complementary public transportation services on behalf of cities, municipalities, and businesses, including fixed route buses and trains, paratransit services, shuttle buses, outsourced vehicle maintenance and other services.Market trends and drivers\nStudent and public transportation are critical components of the US and Canadian educational and economic ecosystems, with student success metrics and funding directly correlated to student attendance, and millions of individuals across the socioeconomic spectrum relying on public transportation daily to get to work, access healthcare, and contribute to the broader economy. As a result of the essential nature of services provided, the public transportation end market is highly stable through economic cycle and benefits from favourable tailwinds including increasing demand for outsourcing and for safer, smarter and more environmentally friendly transportation options. The growing capital intensity and operational complexity associated with digitizing operations and electrifying the fleet will place greater demand across the value chain and favor large, resource-rich outsourced providers, such as First Student and First Transit, who bring best-in-class safety, reliability, capital, and technological expertise to districts and governments.Investment potential\nEQT Infrastructure is committed to building upon the success First Student and First Transit have achieved by making investments in organizational, operational, digital, and sustainability initiatives to further improve and differentiate the Company’s service offering. Most notably, EQT Infrastructure intends to help future-proof the Company by investing in the electrification of its fleet and accelerating its transition to renewable fuel sources in order to support passenger health and reduce environmental impact.",
            "preamble": "First Student (“FS”) is the largest student transportation service provider in North America, providing over 900 million student journeys a year; First Transit (“FT”) is one of the leading transit management operators in North America, transporting over 300 million passengers annually. ",
            "heading": "Leading providers of transportation solutions to communities in North America",
            "responsible_advisors": ["Crosby Cook"],
            "website": "https://firststudentinc.com/",
            "registered_domain": "firststudentinc.com",
            "board": [
                {"title": "Chairperson", "name": "Jake Brace", "person": None},
                {"title": "Board member", "name": "Carol Browner", "person": None},
                {"title": "Board member", "name": "Amy Rosen", "person": None},
                {"title": "Board member", "name": "Nick Costides", "person": None},
                {"title": "Board member", "name": "Crosby Cook", "person": None},
            ],
            "management": [
                {"title": "CEO First Student", "name": "John Kenning", "person": None},
                {"title": "CFO First Student", "name": "Joseph Schwaderer", "person": None},
                {"title": "CEO First Transit", "name": "Brad Thomas", "person": None},
                {"title": "CFO First Transit", "name": "Mark Williams", "person": None},
            ],
            "logo": "https://cdn.sanity.io/images/30p7so6x/eqt-web-prod/bc7c2fe1ae87df14c0d007f5833e77829df0ed23-130x44.png",
        }
        cd = read_json_file("tests/sample_data/company_details.json")
        details = extract_details(cd)
        self.assertEqual(details, expected_results)

    @mock.patch('src.page_data.page_data_sync.fetch_company_details', side_effect=mocked_fetch_company_details_no_details)
    def test_get_companies_no_details(self, _):
        expected_results = [
            {
                "title": "First Student and First Transit",
                "sector": "Transport and logistics",
                "country": "United States",
                "fund": ["EQT Infrastructure V"],
                "entry": "2021-07-01",
                "exit": None,
                "company_details_path": "/current-portfolio/first-student-and-first-transit/",
            },
            {
                "title": "WorkWave",
                "sector": "Technology",
                "country": "United States",
                "fund": ["EQT IX", "EQT VIII"],
                "entry": "2020-08-14",
                "exit": None,
                "company_details_path": "/current-portfolio/workwave/",
            },
            {
                "title": "ManyPets",
                "sector": "Insurance",
                "fund": [],
                "country": "United Kingdom",
                "entry": "2021-06-01",
                "exit": None,
                "company_details_path": "/current-portfolio/manypets/",
            },
        ]
        cps = read_json_file("tests/sample_data/current_portfolio_sample.json")
        data = get_company_data(cps)
        self.assertEqual(data, expected_results)

    @mock.patch('src.page_data.page_data_sync.fetch_company_details', side_effect=mocked_fetch_company_details)
    @mock.patch('src.page_data.page_data_sync.extract_details', side_effect=mocked_extract_details)
    def test_get_companies_mocked_details(self, _mocked_extract_details, _mocked_fetch_company_details):
        expected_results = [
            {
                "title": "First Student and First Transit",
                "sector": "Transport and logistics",
                "country": "United States",
                "fund": ["EQT Infrastructure V"],
                "entry": "2021-07-01",
                "exit": None,
                "company_details_path": "/current-portfolio/first-student-and-first-transit/",
                "extracted_details": "mocked",
            },
            {
                "title": "WorkWave",
                "sector": "Technology",
                "country": "United States",
                "fund": ["EQT IX", "EQT VIII"],
                "entry": "2020-08-14",
                "exit": None,
                "company_details_path": "/current-portfolio/workwave/",
                "extracted_details": "mocked",
            },
            {
                "title": "ManyPets",
                "sector": "Insurance",
                "fund": [],
                "country": "United Kingdom",
                "entry": "2021-06-01",
                "exit": None,
                "company_details_path": "/current-portfolio/manypets/",
                "extracted_details": "mocked",
            },
        ]

        cps = read_json_file("tests/sample_data/current_portfolio_sample.json")
        data = get_company_data(cps)
        self.assertEqual(data, expected_results)


if __name__ == '__main__':
    unittest.main()
