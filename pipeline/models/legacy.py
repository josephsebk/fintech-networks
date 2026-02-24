"""
Import existing hardcoded data from the original HTML dashboard
into the canonical model format.

This lets us bootstrap the system with the verified data that was
manually curated, then layer Tracxn data on top.
"""

from __future__ import annotations

import re
from pipeline.models.schema import (
    Company,
    DataSource,
    Education,
    Founder,
    WorkExperience,
)


# ------------------------------------------------------------------
# The original verified dataset (from index.html matrixData)
# ------------------------------------------------------------------

_RAW_MATRIX = [
    {"name": "Vijay Shekhar Sharma", "company": "Paytm", "edu": "DCE/DTU BE 1998", "work": "India Today, RiverRun Software", "tags": ["tag-dce"]},
    {"name": "Harinder Takhar", "company": "Paytm", "edu": "DCE/DTU BE 1998, INSEAD MBA 2006", "work": "One97 Communications", "tags": ["tag-dce"]},
    {"name": "Yashish Dahiya", "company": "Policybazaar", "edu": "IIT Delhi BTech 1994, IIM-A 1996, INSEAD MBA 2001", "work": "—", "tags": ["tag-iitd", "tag-iim"]},
    {"name": "Sarbvir Singh", "company": "Policybazaar", "edu": "IIT Delhi 1993, IIM-A MBA 1995", "work": "Citi, Emerson", "tags": ["tag-iitd", "tag-iim"]},
    {"name": "Ashneer Grover", "company": "BharatPe", "edu": "IIT Delhi BTech 2004, IIM-A MBA 2006", "work": "AmEx, Kotak, Grofers", "tags": ["tag-iitd", "tag-iim"]},
    {"name": "Shashvat Nakrani", "company": "BharatPe", "edu": "IIT Delhi 2019", "work": "—", "tags": ["tag-iitd"]},
    {"name": "Sameer Nigam", "company": "PhonePe", "edu": "Univ of Mumbai, Univ of Arizona MS, Wharton MBA", "work": "Flipkart SVP, Shopzilla", "tags": ["tag-fk"]},
    {"name": "Rahul Chari", "company": "PhonePe", "edu": "—", "work": "Flipkart", "tags": ["tag-fk"]},
    {"name": "Burzin Engineer", "company": "PhonePe", "edu": "—", "work": "Flipkart", "tags": ["tag-fk"]},
    {"name": "Ajay Bhat", "company": "PhonePe", "edu": "—", "work": "Flipkart", "tags": ["tag-fk"]},
    {"name": "Lalit Keshre", "company": "Groww", "edu": "IIT Bombay BTech+MTech EE 2004", "work": "Flipkart PM, Ittiam, Eduflix", "tags": ["tag-iitb", "tag-fk"]},
    {"name": "Harsh Jain", "company": "Groww", "edu": "IIT Delhi BTech+MTech, UCLA MBA", "work": "Flipkart PM", "tags": ["tag-iitd", "tag-fk"]},
    {"name": "Ishan Bansal", "company": "Groww", "edu": "BITS Pilani", "work": "Flipkart, ICICI Bank", "tags": ["tag-bits", "tag-fk", "tag-icici"]},
    {"name": "Neeraj Singh", "company": "Groww", "edu": "—", "work": "Flipkart", "tags": ["tag-fk"]},
    {"name": "Harshil Mathur", "company": "Razorpay", "edu": "IIT Roorkee", "work": "—", "tags": []},
    {"name": "Shashank Kumar", "company": "Razorpay", "edu": "IIT Roorkee", "work": "—", "tags": []},
    {"name": "Sachin Bansal", "company": "Navi", "edu": "IIT Delhi BTech CS 2005", "work": "Amazon, Flipkart (co-founder)", "tags": ["tag-iitd"]},
    {"name": "Ankit Agarwal", "company": "Navi", "edu": "IIT Delhi BTech 2004, IIM-A MBA 2008", "work": "Bank of America, Deutsche Bank", "tags": ["tag-iitd", "tag-iim"]},
    {"name": "Bipin Preet Singh", "company": "MobiKwik", "edu": "IIT Delhi BTech 2002", "work": "Freescale, NVIDIA, Intel", "tags": ["tag-iitd"]},
    {"name": "Chandan Joshi", "company": "MobiKwik", "edu": "IIT Delhi BTech 2004, UCLA/LBS MBA", "work": "Credit Suisse", "tags": ["tag-iitd"]},
    {"name": "Anurag Sinha", "company": "OneCard", "edu": "IIM Bangalore", "work": "ICICI Bank", "tags": ["tag-iim", "tag-icici"]},
    {"name": "Rupesh Kumar", "company": "OneCard", "edu": "IIT Delhi BTech 1999, ISB MBA 2004", "work": "ICICI Bank", "tags": ["tag-iitd", "tag-icici"]},
    {"name": "Devang Shah", "company": "OneCard", "edu": "—", "work": "ICICI Bank", "tags": ["tag-icici"]},
    {"name": "Vibhav Hathi", "company": "OneCard", "edu": "—", "work": "ICICI Bank", "tags": ["tag-icici"]},
    {"name": "Hari Velayudan", "company": "OneCard", "edu": "—", "work": "Citrus Payment", "tags": []},
    {"name": "Jitendra Gupta", "company": "Jupiter", "edu": "—", "work": "ICICI Bank, Citrus (co-founder), PayU", "tags": ["tag-icici"]},
    {"name": "Amrish Rau", "company": "Pine Labs (CEO)", "edu": "—", "work": "Citrus (co-founder)", "tags": []},
    {"name": "Lokvir Kapoor", "company": "Pine Labs", "edu": "IIT Kanpur, IIM Bangalore", "work": "—", "tags": ["tag-iim"]},
    {"name": "Puneet Agarwal", "company": "Money View", "edu": "IIT Delhi BTech, Purdue MBA", "work": "Google, McKinsey, Capital One", "tags": ["tag-iitd"]},
    {"name": "Sanjay Aggarwal", "company": "Money View", "edu": "IIT Delhi BTech 1993", "work": "Yahoo, Minglebox", "tags": ["tag-iitd"]},
    {"name": "Kavitha Subramanian", "company": "Upstox", "edu": "IIT Bombay, Wharton MBA", "work": "McKinsey", "tags": ["tag-iitb"]},
    {"name": "Sumit Gupta", "company": "CoinDCX", "edu": "IIT Bombay", "work": "—", "tags": ["tag-iitb"]},
    {"name": "Neeraj Khandelwal", "company": "CoinDCX", "edu": "IIT Bombay", "work": "—", "tags": ["tag-iitb"]},
    {"name": "Ravish Naresh", "company": "Khatabook", "edu": "IIT Bombay", "work": "Housing.com", "tags": ["tag-iitb"]},
    {"name": "Ashish Sonone", "company": "Khatabook", "edu": "IIT Bombay", "work": "—", "tags": ["tag-iitb"]},
    {"name": "Dhanesh Kumar", "company": "Khatabook", "edu": "IIT Bombay", "work": "—", "tags": ["tag-iitb"]},
    {"name": "Jaideep Poonia", "company": "Khatabook", "edu": "IIT Bombay", "work": "—", "tags": ["tag-iitb"]},
    {"name": "Sujith Narayanan", "company": "Fi", "edu": "—", "work": "Google", "tags": []},
    {"name": "Sumit Gwalani", "company": "Fi", "edu": "—", "work": "Google", "tags": []},
    {"name": "Ashish Kashyap", "company": "INDMoney", "edu": "—", "work": "Google, ibibo", "tags": []},
    {"name": "Asish Mohapatra", "company": "Oxyzo", "edu": "IIT Kharagpur, ISB", "work": "McKinsey", "tags": []},
    {"name": "Ruchi Kalra", "company": "Oxyzo", "edu": "IIT Delhi BTech 2004, ISB MBA 2007", "work": "McKinsey, Evalueserve", "tags": ["tag-iitd"]},
    {"name": "Rajan Bajaj", "company": "slice", "edu": "IIT Kharagpur", "work": "Flipkart", "tags": ["tag-fk"]},
    {"name": "Deepak Malhotra", "company": "slice", "edu": "BITS Pilani", "work": "—", "tags": ["tag-bits"]},
    {"name": "Anand Prabhudesai", "company": "Turtlemint", "edu": "IIT Bombay", "work": "—", "tags": ["tag-iitb"]},
    {"name": "Dhirendra Mahyavanshi", "company": "Turtlemint", "edu": "—", "work": "ICICI Bank, Quikr", "tags": ["tag-icici"]},
    {"name": "Sumit Maniyar", "company": "Rupeek", "edu": "IIT Bombay", "work": "—", "tags": ["tag-iitb"]},
    {"name": "Ashwin Soni", "company": "Rupeek", "edu": "IIT Bombay", "work": "—", "tags": ["tag-iitb"]},
    {"name": "Nitin Gupta", "company": "PayU", "edu": "IIT Delhi BTech+MTech 2008", "work": "Royal Bank of Scotland", "tags": ["tag-iitd"]},
    {"name": "Sanjeev Srinivasan", "company": "ACKO", "edu": "—", "work": "ICICI Bank", "tags": ["tag-icici"]},
    {"name": "Deena Jacob", "company": "Open", "edu": "—", "work": "ICICI Bank", "tags": ["tag-icici"]},
    {"name": "Anish Achuthan", "company": "Open", "edu": "—", "work": "Citrus Payment", "tags": []},
    {"name": "Mabel Chacko", "company": "Open", "edu": "IIM Bangalore", "work": "—", "tags": ["tag-iim"]},
]


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _parse_education(edu_str: str) -> list[Education]:
    """Best-effort parse of the freeform education strings."""
    if edu_str == "—" or not edu_str:
        return []
    entries = []
    for part in edu_str.split(","):
        part = part.strip()
        # Try to extract year
        year_match = re.search(r"\b(19|20)\d{2}\b", part)
        year = int(year_match.group()) if year_match else None
        # Try to extract institution
        institution = re.sub(r"\b(BTech|MTech|BE|MBA|MS|BSc|BA)\b.*", "", part).strip()
        # Try to extract degree
        degree_match = re.search(r"\b(BTech|MTech|BE|MBA|MS|BSc|BA)\b", part)
        degree = degree_match.group() if degree_match else None
        if institution:
            entries.append(Education(institution=institution, degree=degree, year=year))
    return entries


def _parse_work(work_str: str) -> list[WorkExperience]:
    """Best-effort parse of the freeform work strings."""
    if work_str == "—" or not work_str:
        return []
    entries = []
    for part in work_str.split(","):
        part = part.strip()
        if part:
            entries.append(WorkExperience(company=part))
    return entries


def load_legacy_data() -> tuple[list[Company], list[Founder]]:
    """
    Load the hardcoded verified dataset, returning (companies, founders).
    """
    companies_map: dict[str, Company] = {}
    founders: list[Founder] = []

    for row in _RAW_MATRIX:
        # Company
        company_name = row["company"].replace(" (CEO)", "")
        company_slug = _slug(company_name)
        if company_slug not in companies_map:
            companies_map[company_slug] = Company(
                id=company_slug,
                name=company_name,
                sector="FinTech",
                country="India",
                source=DataSource.MANUAL,
            )

        # Founder
        founder_slug = _slug(f"{row['name']}-{company_slug}")
        founder = Founder(
            id=founder_slug,
            name=row["name"],
            companies=[company_slug],
            education=_parse_education(row["edu"]),
            work_history=_parse_work(row["work"]),
            source=DataSource.MANUAL,
            verified=True,
            tags=row.get("tags", []),
        )
        founders.append(founder)

        # Link founder to company
        if founder_slug not in companies_map[company_slug].founders:
            companies_map[company_slug].founders.append(founder_slug)

    return list(companies_map.values()), founders
