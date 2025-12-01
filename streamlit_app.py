import streamlit as st

#!/usr/bin/env python3
"""
DE-CIX and PeeringDB Data Comparison Tool - FIXED VERSION

FIXES APPLIED:
1. Fixed Name extraction from PeeringDB data structure
2. Improved error handling for ASN lookups that return 404
3. Better handling of missing net data in PeeringDB responses
4. Added batch fetching of network details to fix missing net data issue (depth=2 failure)
"""

import requests
import asyncio
import aiohttp
import pandas as pd
from config import API_KEY  # Expects a config.py with API_KEY = "pdb_..."
from typing import Dict, Any, List, Set

# --- Constants ---

PEERINGDB_HEADERS = {
    "Authorization": f"Api-Key {API_KEY}"
}

# Rate limiting: PeeringDB allows ~100 requests per minute
# BUT: Each ASN requires 2+ requests (net + poc), so we need to be very conservative
# Strategy: Use minimal concurrency with longer delays
RATE_LIMIT_DELAY = 2.0  # 2 seconds between each request = 30 req/min
MAX_CONCURRENT_REQUESTS = 3  # Only 3 concurrent to stay safe

# --- Asynchronous HTTP Functions ---

async def fetch_json_data(session, url, headers=None, retry_count=0, max_retries=3):
    """
    Asynchronously fetches JSON data from a given URL with retry logic for 429 errors.
    """
    try:
        async with session.get(url, headers=headers) as response:
            # If we get a 429, wait longer and retry
            if response.status == 429 and retry_count < max_retries:
                wait_time = 10 * (retry_count + 1)  # 10s, 20s, 30s
                print(f"Rate limited. Waiting {wait_time}s before retry {retry_count + 1}/{max_retries}...")
                await asyncio.sleep(wait_time)
                return await fetch_json_data(session, url, headers, retry_count + 1, max_retries)
            
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        if "429" not in str(e) or retry_count >= max_retries:
            print(f"Error fetching data from {url}: {e}")
        return None

async def fetch_contacts_for_asn(session, asn: int, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
    """
    Asynchronously fetches contact details for a specific ASN.
    This is used for ASNs that were not in the initial IX-wide pull.
    
    FIX: Returns None if ASN doesn't exist in PeeringDB (404), so caller
    can handle it appropriately (e.g., skip adding to contact_map)
    
    FIX: Added semaphore for rate limiting to prevent 429 errors
    """
    async with semaphore:
        contacts = {'noc': set(), 'technical': set(), 'policy': set()}
        
        # 1. Find ALL net_ids for the ASN
        net_url = f"https://www.peeringdb.com/api/net?asn={asn}"
        
        # Add delay to respect rate limits
        await asyncio.sleep(RATE_LIMIT_DELAY)
        net_data = await fetch_json_data(session, net_url, headers=PEERINGDB_HEADERS)
        
        # FIX: If we get None (404) or empty data, return None to signal ASN doesn't exist
        if net_data is None or not net_data.get('data'):
            return None
            
        net_entries = net_data['data']
        name = net_entries[0].get('name', f"ASN {asn}") 
        net_ids = [entry['id'] for entry in net_entries if entry.get('id')]

        # 2. Fetch contacts for EACH net_id
        for net_id in net_ids:
            poc_url = f"https://www.peeringdb.com/api/poc?net_id={net_id}"
            
            # Add delay between POC requests too
            await asyncio.sleep(RATE_LIMIT_DELAY)
            poc_data = await fetch_json_data(session, poc_url, headers=PEERINGDB_HEADERS)
            
            if poc_data and poc_data.get('data'):
                poc_list = poc_data.get('data', [])
                
                for contact in poc_list:
                    role = str(contact.get('role')).lower()
                    email = contact.get('email')
                    
                    if not email:
                        continue
                    
                    if role == 'noc':
                        contacts['noc'].add(email)
                    elif role == 'technical':
                        contacts['technical'].add(email)
                    elif role == 'policy':
                        contacts['policy'].add(email)
                    
        return {
            'asn': asn,
            'name': name,
            'contacts': {
                'noc': list(contacts['noc']),
                'technical': list(contacts['technical']),
                'policy': list(contacts['policy'])
            }
        }

async def fetch_network_details_batch(session, net_ids: List[int]) -> Dict[int, Any]:
    """
    Fetches network details for a list of net_ids in batches.
    Returns a dictionary mapping net_id to network data (including contacts).
    """
    network_map = {}
    if not net_ids:
        return network_map
        
    # Chunk net_ids to avoid URL length limits (e.g., 50 at a time)
    chunk_size = 50
    for i in range(0, len(net_ids), chunk_size):
        chunk = net_ids[i:i + chunk_size]
        ids_str = ",".join(map(str, chunk))
        
        # Fetch net details with depth=2 to get contacts (poc_set)
        url = f"https://www.peeringdb.com/api/net?id__in={ids_str}&depth=2"
        
        # Add delay to respect rate limits
        await asyncio.sleep(RATE_LIMIT_DELAY)
        data = await fetch_json_data(session, url, headers=PEERINGDB_HEADERS)
        
        if data and data.get('data'):
            for net in data['data']:
                network_map[net['id']] = net
                
    return network_map

# --- Data Processing Functions ---

def process_customer_location_data(data):
    """Processes the Customer Location JSON data, collecting all IPs for an ASN."""
    processed_data = {}
    if not data or "member_list" not in data:
        return processed_data

    for member in data["member_list"]:
        asn = member.get("asnum")
        name = member.get("name", "")
        
        # Skip DE-CIX entries
        if not asn or "DE-CIX" in name or "DE-CIX" in str(member.get("name", "")):
            continue

        if asn not in processed_data:
            processed_data[asn] = {
                "name": name,
                "ipv4_list": set(),
                "ipv6_list": set(),
                "source": "customer"
            }

        for conn in member.get("connection_list", []):
            for vlan in conn.get("vlan_list", []):
                ipv4 = vlan.get("ipv4", {}).get("address")
                ipv6 = vlan.get("ipv6", {}).get("address")
                if ipv4:
                    processed_data[asn]["ipv4_list"].add(ipv4)
                if ipv6:
                    processed_data[asn]["ipv6_list"].add(ipv6)
    return processed_data

def process_lg_neighbors_data(data):
    """Processes the LG Neighbors JSON data."""
    processed_data = {}
    if not data or "neighbors" not in data:
        return processed_data
        
    for neighbor in data["neighbors"]:
        asn = neighbor.get("asn")
        description = neighbor.get("description", "")
        
        # Skip DE-CIX entries
        if asn and "DE-CIX" not in description and "DE-CIX" not in str(neighbor.get("description", "")):
            processed_data[asn] = {
                "name": description,
                "ipv4_list": {neighbor.get("address")},
                "ipv6_list": set(),
                "source": "lg"
            }
    return processed_data

def merge_decix_data(customer_data, lg_data):
    """Merges the two DE-CIX JSON datasets and finds conflicts."""
    merged_list = {}
    conflicts = {}
    all_asns = set(customer_data.keys()) | set(lg_data.keys())

    for asn in all_asns:
        in_customer = asn in customer_data
        in_lg = asn in lg_data

        if in_customer and in_lg:
            # Conflict if there's no overlap in IPv4 addresses
            if not customer_data[asn]["ipv4_list"].intersection(lg_data[asn]["ipv4_list"]):
                conflicts[asn] = {
                    "name": customer_data[asn]["name"],
                    "customer_location": customer_data[asn],
                    "lg_neighbors": lg_data[asn],
                }
            # Prefer customer location data for the merged list as it's richer
            merged_list[asn] = customer_data[asn]
        elif in_customer:
            merged_list[asn] = customer_data[asn]
        elif in_lg:
            merged_list[asn] = lg_data[asn]

    return merged_list, conflicts

def fetch_peeringdb_data(data, network_map=None):
    """
    Processes PeeringDB data, including embedded contact info from depth=2.
    
    FIX: Properly extracts name from the nested structure.
    PeeringDB netixlan API with depth=2 returns data like:
    {
        "data": [
            {
                "id": 12345,
                "asn": 12345,
                "ipaddr4": "1.2.3.4",
                "ipaddr6": "2001:...",
                "net": {
                    "id": 67890,
                    "name": "Company Name",
                    "poc_set": [...]
                }
            }
        ]
    }
    """
    processed_data = {}
    if not data or "data" not in data:
        return processed_data

    # This extensive list is used to filter DE-CIX-owned ASNs
    DE_CIX_ASN = {
        6695, 57769, 43252, 47228, 25083, 20717, 20715, 61374, 56890, 62499, 
        48793, 205529, 43729, 207926, 30010, 56858, 206780, 212723, 54716, 
        62760, 65512, 9748, 57891, 4617, 57802, 38137, 38175, 38194, 38206, 
        393752, 210586, 30504, 63034, 400330, 149008, 38208, 204682, 400549, 
        204034, 400548, 54386, 53952, 204663, 201526, 201537, 201561, 201560, 
        42476, 37793, 201410, 200225, 65300, 37795, 37799, 207515, 25736, 
        211100, 133086, 200779, 65460, 205818, 207334, 198116, 140307, 151829, 
        135382, 64989, 152587, 272394, 396471, 274682, 274703, 272395, 211942, 
        274739, 274751, 215791, 8960, 8961, 65095, 65532, 65531, 65530, 
        205530, 196610, 51531, 214292
    }

    for entry in data["data"]:
        asn = entry.get("asn")
        
        if not asn:
            continue
        
        # Filter 1: By ASN
        if asn in DE_CIX_ASN:
            continue

        # FIX: Properly handle the net_data structure
        # If network_map is provided, look up by net_id
        net_data = None
        if network_map:
            net_id = entry.get("net_id")
            if net_id in network_map:
                net_data = network_map[net_id]
        
        # Fallback to embedded 'net' if not found in map
        if not net_data:
            net_data = entry.get("net")
        
        # Extract name - try from net_data first, then entry, then default
        if net_data and isinstance(net_data, dict):
            name = net_data.get("name")
        else:
            name = entry.get("name")
        
        # If still no name, create a default
        if not name:
            name = f"ASN {asn}"
        
        # Filter 2: By Name (after we've properly extracted it)
        if "DE-CIX" in str(name).upper():
            continue
        
        # Extract contacts from the net_data structure
        contacts = {'noc': set(), 'technical': set(), 'policy': set()}
        
        if net_data and isinstance(net_data, dict):
            poc_set = net_data.get('poc_set', [])
            
            for contact in poc_set:
                role = str(contact.get('role')).lower() 
                email = contact.get('email')
                
                if not email:
                    continue
                    
                if role == 'noc':
                    contacts['noc'].add(email)
                elif role == 'technical':
                    contacts['technical'].add(email)
                elif role == 'policy': 
                    contacts['policy'].add(email)
        
        processed_data[asn] = {
            "name": name, 
            "ipv4": entry.get("ipaddr4"),
            "ipv6": entry.get("ipaddr6"),
            "contacts": {
                "noc": list(contacts['noc']),
                "technical": list(contacts['technical']),
                "policy": list(contacts['policy']) 
            }
        }
    return processed_data

def compare_data(merged_decix, peeringdb_data):
    """Compares the merged DE-CIX data with PeeringDB data."""
    missing_in_peeringdb = {}
    missing_in_json = {}
    ip_mismatches = {}

    decix_asns = set(merged_decix.keys())
    peeringdb_asns = set(peeringdb_data.keys())

    # Find what's in DE-CIX json but not in PeeringDB at all (ASN doesn't exist *at this IX*)
    for asn in decix_asns - peeringdb_asns:
        missing_in_peeringdb[asn] = merged_decix[asn]

    # Find what's in PeeringDB but not in DE-CIX
    for asn in peeringdb_asns - decix_asns:
        missing_in_json[asn] = peeringdb_data[asn]

    # For common ASNs, check for IP mismatches
    for asn in decix_asns & peeringdb_asns:
        decix_entry = merged_decix[asn]
        pdb_entry = peeringdb_data[asn]
        
        ipv4_mismatch = False
        ipv6_mismatch = False
        
        if pdb_entry.get("ipv4"):
            if decix_entry.get("ipv4_list") and pdb_entry["ipv4"] not in decix_entry["ipv4_list"]:
                ipv4_mismatch = True
        
        if pdb_entry.get("ipv6"):
            if decix_entry.get("ipv6_list") and pdb_entry["ipv6"] not in decix_entry["ipv6_list"]:
                ipv6_mismatch = True

        if ipv4_mismatch or ipv6_mismatch:
            ip_mismatches[asn] = {
                "name": decix_entry.get("name") or pdb_entry.get("name"),
                "json_data": decix_entry,
                "peeringdb_data": pdb_entry,
                "ipv4_mismatch": ipv4_mismatch,
                "ipv6_mismatch": ipv6_mismatch
            }

    return missing_in_peeringdb, missing_in_json, ip_mismatches

# --- Reporting & Export Functions ---

def format_contacts_for_print(contacts: Dict[str, List[str]]) -> str:
    """Formats a contact dictionary for clean printing."""
    lines = []
    noc_emails = contacts.get('noc', [])
    tech_emails = contacts.get('technical', [])
    
    if noc_emails:
        lines.append(f"   NOC: {', '.join(noc_emails)}")
    if tech_emails:
        lines.append(f"   Tech: {', '.join(tech_emails)}")
        
    if not lines:
        return "   Contacts: None found"
        
    return "\n".join(lines)

def print_results(missing_in_peeringdb, missing_in_json, ip_mismatches, conflicts, verbose, contact_map):
    """Prints the per-ASN comparison results to the console."""
    print("\n" + "="*60)
    print("COMPARISON RESULTS (PER-ASN)")
    print("="*60)
    
    print("\n--- Missing in PeeringDB (Not listed at this IX in PDB) ---")
    if not verbose:
        print(f"Found {len(missing_in_peeringdb)} entries.")
    else:
        if missing_in_peeringdb:
            for asn, data in sorted(missing_in_peeringdb.items()):
                print(f"ASN {asn}   Name: {data['name']}")
                print(f"   JSON: {', '.join(data.get('ipv4_list')) or 'N/A'} / {', '.join(data.get('ipv6_list')) or 'N/A'}")
                contact_info = contact_map.get(asn)
                if contact_info:
                    print(format_contacts_for_print(contact_info.get('contacts', {})))
                else:
                    print("   Contacts: ASN not found in PeeringDB (404)")
        else:
            print("None")

    print("\n--- Missing in JSON merged list (In PDB but not DE-CIX lists) ---")
    if not verbose:
        print(f"Found {len(missing_in_json)} entries.")
    else:
        if missing_in_json:
            for asn, data in sorted(missing_in_json.items()):
                if "DE-CIX" in str(data.get('name', '')).upper():
                    continue
                # FIX: Name should now be properly extracted
                print(f"ASN {asn}   Name: {data['name']}")
                print(f"   PeeringDB: {data.get('ipv4') or 'N/A'} / {data.get('ipv6') or 'N/A'}")
                print(format_contacts_for_print(data.get('contacts', {})))
        else:
            print("None")
            
    print("\n--- IP Mismatches (JSON vs PeeringDB) ---")
    if not verbose:
        print(f"Found {len(ip_mismatches)} mismatches.")
    else:
        if ip_mismatches:
            for asn, data in sorted(ip_mismatches.items()):
                print(f"ASN {asn}   Name: {data['name']}")
                if data.get('ipv4_mismatch'):
                    print(f"   IPv4 - JSON: {', '.join(data['json_data'].get('ipv4_list')) or 'N/A'}")
                    print(f"          PDB:  {data['peeringdb_data'].get('ipv4') or 'N/A'}")
                if data.get('ipv6_mismatch'):
                    print(f"   IPv6 - JSON: {', '.join(data['json_data'].get('ipv6_list')) or 'N/A'}")
                    print(f"          PDB:  {data['peeringdb_data'].get('ipv6') or 'N/A'}")
                
                print(format_contacts_for_print(data['peeringdb_data'].get('contacts', {})))
        else:
            print("None")

    print("\n--- Conflicts between JSON sources ---")
    if not verbose:
        print(f"Found {len(conflicts)} conflicts.")
    else:
        if conflicts:
            for asn, data in sorted(conflicts.items()):
                print(f"ASN {asn}   Name: {data['name']}")
                print(f"   Customer Location JSON: {', '.join(data['customer_location'].get('ipv4_list')) or 'N/A'} / {', '.join(data['customer_location'].get('ipv6_list')) or 'N/A'}")
                print(f"   LG Neighbors JSON: {', '.join(data['lg_neighbors'].get('ipv4_list')) or 'N/A'} / {', '.join(data['lg_neighbors'].get('ipv6_list')) or 'N/A'}")
        else:
            print("None")

def get_user_input():
    """Gets configuration from user input with example guidance."""
    print("\n" + "="*60)
    print("DE-CIX and PeeringDB Data Comparison Tool")
    print("="*60)
    print("\nThis tool compares DE-CIX portal and looking glass data with PeeringDB.")
    print("It will automatically filter out DE-CIX's own entries from all sources.\n")
    
    print("Example configuration for DE-CIX Chicago:")
    print("   - Location code: ord")
    print("   - Route server: rs1_ord_ipv4")
    print("   - PeeringDB IXP ID: 3378")
    print("\nTo find these values:")
    print("   - Location codes: nyc (New York), ord (Chicago), fra (Frankfurt), etc.")
    print("   - Route server format: rs1_LOCATION_ipv4 or rs1_LOCATION_ipv6")
    print("   - PeeringDB IXP ID: Go to peeringdb.com, search for your IX, look for 'ix/XXX' in URL\n")
    
    print("-"*60)
    location = input("Enter DE-CIX location code (e.g., 'ord' for Chicago): ").strip()
    if not location:
        location = "ord"
        print(f"Using default: {location}")
    
    routeserver = input(f"Enter route server (e.g., 'rs1_{location}_ipv4'): ").strip()
    if not routeserver:
        routeserver = f"rs1_{location}_ipv4"
        print(f"Using default: {routeserver}")
    
    ixp_id_str = input("Enter PeeringDB IXP ID (e.g., '3378' for DE-CIX Chicago): ").strip()
    if not ixp_id_str:
        ixp_id = 3378
        print(f"Using default: {ixp_id}")
    else:
        try:
            ixp_id = int(ixp_id_str)
        except ValueError:
            print("Invalid IXP ID. Using default: 3378")
            ixp_id = 3378
    
    verbose_input = input("\nShow detailed output? (y/N): ").strip().lower()
    verbose = verbose_input in ['y', 'yes']
    
    return location, routeserver, ixp_id, verbose

def export_to_excel(filename, missing_in_peeringdb, missing_in_json, ip_mismatches, conflicts, contact_map):
    """
    Exports all comparison data to a multi-sheet Excel file.
    This function will consolidate the data by company name using pandas.groupby().
    """
    
    def agg_str_list(series):
        """Aggregates a series of strings or sets into a single comma-separated string."""
        final_set = set()
        for item in series.dropna(): 
            if isinstance(item, (set, list)):
                final_set.update(item)
            elif isinstance(item, str) and item:
                final_set.update(email.strip() for email in item.split(','))
            elif isinstance(item, (int, float)):
                 final_set.add(str(item))
        
        try:
            return ", ".join(sorted(final_set, key=int))
        except ValueError:
            return ", ".join(sorted(final_set))

    # Rules for "Missing in PDB" (has JSON IPs, no PDB IPs)
    agg_rules_mip = {
        'ASNs': agg_str_list, 'JSON IPv4': agg_str_list, 'JSON IPv6': agg_str_list,
        'NOC Email': agg_str_list, 'Technical Email': agg_str_list, 'Policy Email': agg_str_list,
    }
    
    # Rules for "Missing in JSON" (has PDB IPs, no JSON IPs)
    agg_rules_mij = {
        'ASNs': agg_str_list, 'PeeringDB IPv4': agg_str_list, 'PeeringDB IPv6': agg_str_list,
        'NOC Email': agg_str_list, 'Technical Email': agg_str_list, 'Policy Email': agg_str_list,
    }
    
    # Rules for "IP Mismatches" (has all IPs)
    agg_rules_ipm = {
        'ASNs': agg_str_list, 'JSON IPv4': agg_str_list, 'JSON IPv6': agg_str_list,
        'PeeringDB IPv4': agg_str_list, 'PeeringDB IPv6': agg_str_list,
        'NOC Email': agg_str_list, 'Technical Email': agg_str_list, 'Policy Email': agg_str_list,
    }

    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            
            # --- Sheet 1: Missing in PeeringDB (Consolidated) ---
            mip_raw_data = []
            for asn, data in missing_in_peeringdb.items():
                contact_info = contact_map.get(asn, {})
                name = contact_info.get('name') or data.get('name') or f"ASN {asn}"
                contacts = contact_info.get('contacts', {})
                mip_raw_data.append({
                    "Company Name": name, "ASNs": str(asn),
                    "JSON IPv4": data.get('ipv4_list', set()),
                    "JSON IPv6": data.get('ipv6_list', set()),
                    "NOC Email": ", ".join(sorted(contacts.get('noc', []))),
                    "Technical Email": ", ".join(sorted(contacts.get('technical', []))),
                    "Policy Email": ", ".join(sorted(contacts.get('policy', []))),
                })
            
            df_mip = pd.DataFrame(mip_raw_data)
            if not df_mip.empty:
                df_mip_grouped = df_mip.groupby('Company Name').agg(agg_rules_mip).reset_index()
            else:
                df_mip_grouped = pd.DataFrame(columns=["Company Name"] + list(agg_rules_mip.keys()))
            df_mip_grouped.to_excel(writer, sheet_name='Missing_in_PDB', index=False)

            # --- Sheet 2: Missing in JSON (Consolidated) ---
            mij_raw_data = []
            for asn, data in missing_in_json.items():
                if "DE-CIX" in str(data.get('name', '')).upper(): continue
                contacts = data.get('contacts', {})
                mij_raw_data.append({
                    "Company Name": data.get('name'), "ASNs": str(asn),
                    "PeeringDB IPv4": data.get('ipv4'),
                    "PeeringDB IPv6": data.get('ipv6'),
                    "NOC Email": ", ".join(sorted(contacts.get('noc', []))),
                    "Technical Email": ", ".join(sorted(contacts.get('technical', []))),
                    "Policy Email": ", ".join(sorted(contacts.get('policy', []))),
                })
            
            df_mij = pd.DataFrame(mij_raw_data)
            if not df_mij.empty:
                df_mij_grouped = df_mij.groupby('Company Name').agg(agg_rules_mij).reset_index()
            else:
                df_mij_grouped = pd.DataFrame(columns=["Company Name"] + list(agg_rules_mij.keys()))
            df_mij_grouped.to_excel(writer, sheet_name='Missing_in_JSON', index=False)

            # --- Sheet 3: IP Mismatches (Consolidated) ---
            ipm_raw_data = []
            for asn, data in ip_mismatches.items():
                contacts = data['peeringdb_data'].get('contacts', {})
                ipm_raw_data.append({
                    "Company Name": data.get('name'), "ASNs": str(asn),
                    "JSON IPv4": data['json_data'].get('ipv4_list', set()),
                    "JSON IPv6": data['json_data'].get('ipv6_list', set()),
                    "PeeringDB IPv4": data['peeringdb_data'].get('ipv4'),
                    "PeeringDB IPv6": data['peeringdb_data'].get('ipv6'),
                    "NOC Email": ", ".join(sorted(contacts.get('noc', []))),
                    "Technical Email": ", ".join(sorted(contacts.get('technical', []))),
                    "Policy Email": ", ".join(sorted(contacts.get('policy', []))),
                })
            
            df_ipm = pd.DataFrame(ipm_raw_data)
            if not df_ipm.empty:
                df_ipm_grouped = df_ipm.groupby('Company Name').agg(agg_rules_ipm).reset_index()
            else:
                df_ipm_grouped = pd.DataFrame(columns=["Company Name"] + list(agg_rules_ipm.keys()))
            df_ipm_grouped.to_excel(writer, sheet_name='IP_Mismatches', index=False)
            
            # --- Sheet 4: JSON Conflicts (still per-ASN) ---
            conf_data = []
            for asn, data in sorted(conflicts.items()):
                conf_data.append({
                    "ASN": asn,
                    "Company Name": data.get('name'),
                    "Customer_Loc_IPv4": ", ".join(data['customer_location'].get('ipv4_list', set())),
                    "Customer_Loc_IPv6": ", ".join(data['customer_location'].get('ipv6_list', set())),
                    "LG_Neighbors_IPv4": ", ".join(data['lg_neighbors'].get('ipv4_list', set())),
                    "LG_Neighbors_IPv6": ", ".join(data['lg_neighbors'].get('ipv6_list', set())),
                })
            
            df_conf = pd.DataFrame(conf_data)
            df_conf.to_excel(writer, sheet_name='JSON_Conflicts', index=False)
            
    except Exception as e:
        print(f"\n--- ERROR writing to Excel file ---")
        print(f"Error: {e}")
        print("Please ensure 'pandas' and 'openpyxl' are installed (`pip install pandas openpyxl`)")
        print("Also, make sure the file is not already open in Excel.")

# --- Main Execution ---

async def main():
    """Main function to run the comparison."""
    
    # Get user input
    location, routeserver, ixp_id, verbose = get_user_input()
    
    print("\n" + "-"*60)
    print(f"Configuration:")
    print(f"   Location: {location}")
    print(f"   Route Server: {routeserver}")
    print(f"   PeeringDB IXP ID: {ixp_id}")
    print(f"   Verbose Mode: {verbose}")
    print("-"*60)
    
    # Define URLs based on user input
    customer_location_url = f"https://portal.de-cix.net/customer_{location}"
    lg_neighbors_url = f"https://lg.de-cix.net/api/v1/routeservers/{routeserver}/neighbors"
    peeringdb_netixlan_url = f"https://www.peeringdb.com/api/netixlan?ix_id={ixp_id}&depth=2"
    
    print("\nFetching data from:")
    print(f"   1. Customer Portal: {customer_location_url}")
    print(f"   2. Looking Glass: {lg_neighbors_url}")
    print(f"   3. PeeringDB: {peeringdb_netixlan_url}")
    print("\nPlease wait...")

    async with aiohttp.ClientSession() as session:
        # 1. Fetch all data concurrently
        tasks = [
            fetch_json_data(session, customer_location_url),
            fetch_json_data(session, lg_neighbors_url),
            fetch_json_data(session, peeringdb_netixlan_url, headers=PEERINGDB_HEADERS)
        ]
        customer_location_json, lg_neighbors_json, pdb_json = await asyncio.gather(*tasks)

        # 2. Process the raw JSON data
        
        # FIX: Fetch network details separately since depth=2 might fail to expand 'net'
        network_map = {}
        if pdb_json and pdb_json.get('data'):
            net_ids = [entry['net_id'] for entry in pdb_json['data'] if entry.get('net_id')]
            # Remove duplicates
            net_ids = list(set(net_ids))
            if net_ids:
                print(f"\nFetching details for {len(net_ids)} networks from PeeringDB (to ensure correct names)...")
                network_map = await fetch_network_details_batch(session, net_ids)
        
        customer_data = process_customer_location_data(customer_location_json)
        lg_data = process_lg_neighbors_data(lg_neighbors_json)
        peeringdb_data = fetch_peeringdb_data(pdb_json, network_map)

        print(f"\nData retrieved:")
        print(f"   Customer Portal entries: {len(customer_data)} (DE-CIX filtered out)")
        print(f"   Looking Glass entries: {len(lg_data)} (DE-CIX filtered out)")
        print(f"   PeeringDB entries: {len(peeringdb_data)} (DE-CIX filtered out)")

        # 3. Merge DE-CIX data and find conflicts
        merged_decix, conflicts = merge_decix_data(customer_data, lg_data)

        # 4. Compare the datasets
        missing_in_peeringdb, missing_in_json, ip_mismatches = compare_data(merged_decix, peeringdb_data)

        # 5. Build the final contact map
        contact_map = {}
        
        # Add contacts for ASNs we already have from the IX-wide query
        for asn in (set(missing_in_json.keys()) | set(ip_mismatches.keys())):
            if asn in peeringdb_data:
                contact_map[asn] = peeringdb_data[asn]
        
        # Now, fetch contacts ONLY for the ASNs that were missing from PDB's IX list
        asns_to_fetch_contacts = set(missing_in_peeringdb.keys())
        if asns_to_fetch_contacts:
            print(f"\nFetching contact details for {len(asns_to_fetch_contacts)} ASNs missing from PeeringDB's IX list...")
            print(f"   (Rate limited to prevent 429 errors)")
            print(f"   Estimated time: ~{(len(asns_to_fetch_contacts) * 4) // 60} minutes (this is slow but ensures complete data)")
            
            # FIX: Create a semaphore to limit concurrent requests
            # Balance between speed and respecting rate limits
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
            
            # FIX: Create tasks with ASN tracking and semaphore
            tasks_with_asns = [(asn, fetch_contacts_for_asn(session, asn, semaphore)) for asn in asns_to_fetch_contacts]
            contact_results = await asyncio.gather(*[task for _, task in tasks_with_asns])
            
            # FIX: Track ASNs that truly don't exist in PeeringDB at all
            asns_not_in_pdb_at_all = []
            
            for (asn, _), result in zip(tasks_with_asns, contact_results):
                # If result is None, the ASN doesn't exist in PeeringDB anywhere
                if result is None:
                    asns_not_in_pdb_at_all.append(asn)
                    # Don't add to contact_map - leave it empty so display shows "not found"
                else:
                    contact_map[result['asn']] = result
            
            if asns_not_in_pdb_at_all:
                print(f"   Note: {len(asns_not_in_pdb_at_all)} ASN(s) not found in PeeringDB at all:")
                if len(asns_not_in_pdb_at_all) <= 20:
                    for asn in sorted(asns_not_in_pdb_at_all):
                        print(f"      - ASN {asn}")
                else:
                    # If too many, just show first 20
                    for asn in sorted(asns_not_in_pdb_at_all)[:20]:
                        print(f"      - ASN {asn}")
                    print(f"      ... and {len(asns_not_in_pdb_at_all) - 20} more")
            print("   ...done.")
        else:
            print("\nNo ASNs were missing from PeeringDB's IX list; no extra contact fetching needed.")

        # 6. Print the results to console
        print_results(missing_in_peeringdb, missing_in_json, ip_mismatches, conflicts, verbose, contact_map)

        # 7. Prompt for Excel Export
        print("\n" + "="*60)
        try:
            export_input = input("Do you want to export these results to an Excel file? (y/N): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nNo input received. Skipping Excel export.")
            export_input = 'n'
            
        if export_input in ['y', 'yes']:
            filename = f"de-cix_{location}_pdb_{ixp_id}_comparison.xlsx"
            print(f"\nExporting results to {filename}...")
            export_to_excel(
                filename, 
                missing_in_peeringdb, 
                missing_in_json, 
                ip_mismatches, 
                conflicts, 
                contact_map
            )
            print("...Export complete.")

if __name__ == "__main__":
    asyncio.run(main())

st.title("🎈 My new app")
st.write(
    "Let's start building! For help and inspiration, head over to [docs.streamlit.io](https://docs.streamlit.io/)."
)
