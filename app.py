from flask import Flask, request, jsonify, render_template
import sqlite3
import requests
import json
import hashlib
import base64
from urllib.parse import quote
from time import sleep
import re
from unidecode import unidecode
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

app = Flask(__name__)

CLIENT_ID = ''
CLIENT_SECRET = ''

# Rate limiting for avatar requests
avatar_rate_limit_per_second = 10
avatar_request_interval = 1 / avatar_rate_limit_per_second
avatar_last_request_time = [time.time()]
avatar_request_lock = threading.Lock()

def fetch_guild_data(character_name, realm, access_token):
    # First get character data to find guild
    char_url = f"https://eu.api.blizzard.com/profile/wow/character/{realm}/{character_name.lower()}?namespace=profile-eu&locale=en_EU"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    try:
        char_response = requests.get(char_url, headers=headers)
        char_response.raise_for_status()
        char_data = char_response.json()

        if 'guild' not in char_data:
            return None

        guild_name = char_data['guild']['name']
        # Guild realm slug is usually provided directly by the API if the character is in a guild
        guild_realm_slug = char_data['guild']['realm']['slug']

        # Guild names in API URLs are typically slugified (lowercase, spaces to hyphens)
        guild_name_slug = guild_name.lower().replace(' ', '-')

        # guild_url = f"https://eu.api.blizzard.com/data/wow/guild/{guild_realm_slug}/{guild_name_slug}/roster?namespace=profile-eu&locale=en_EU"
        # More robust: use guild ID if available from character guild data, otherwise use name slug.
        # For now, assuming name slug is the primary way if ID isn't readily available or used.
        # The current structure relies on guild name and realm slug:
        guild_url = f"https://eu.api.blizzard.com/data/wow/guild/{guild_realm_slug}/{guild_name_slug}/roster?namespace=profile-eu&locale=en_EU"

        guild_response = requests.get(guild_url, headers=headers)
        guild_response.raise_for_status()
        guild_data = guild_response.json()

        members = []
        for member in guild_data['members']:
            name = member['character']['name']
            # Member realm slug is also available
            # realm_slug_member = member['character']['realm']['slug']
            # Using the name as it's for display and search key, slugification happens in JS/form
            m_realm = member['character']['realm']['slug'] # Use slug for consistency if making further API calls

            search_url = f"/search_guild_member?character_name={quote(name)}&realm={quote(m_realm)}"
            members.append({
                'name': name,
                'realm': m_realm, # Store/use the slug for realm
                'level': member['character']['level'],
                'search_url': search_url
            })

        return {
            'name': guild_name,
            'realm': guild_realm_slug, # Return the slug
            'members': members
        }

    except requests.exceptions.RequestException as e:
        # Log error e for debugging
        return None

def get_account_id(character_name, realm):
    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    c.execute('SELECT account_id FROM characters WHERE character_name = ? AND realm = ?', (character_name, realm))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

def get_account_characters(account_id):
    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    c.execute('SELECT character_name, realm FROM characters WHERE account_id = ?', (account_id,))
    characters = c.fetchall()
    conn.close()
    return characters


@app.route('/search_entire_guild', methods=['POST'])
def search_entire_guild():
    # Password protection
    password = request.form.get('password', '').strip()
    if password != 'imraimramakaveli':
        return jsonify({'success': False, 'message': 'Invalid password.'})

    character_name = request.form.get('character_name', '').strip().lower()
    realm = request.form.get('realm', '').strip().lower().replace(' ', '-')

    if not character_name or not realm:
        return jsonify({'success': False, 'message': 'Character name and realm are required.'})

    access_token = get_access_token()
    if not access_token:
        return jsonify({'success': False, 'message': 'Failed to get access token.'})

    # Get guild info first
    guild_info = fetch_guild_data(character_name, realm, access_token)
    if not guild_info or not guild_info.get('members'):
        return jsonify({'success': False, 'message': 'Guild not found or has no accessible members.'})

    total_members = len(guild_info['members'])

    # Get existing characters from DB to avoid unnecessary API calls
    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    c.execute('SELECT character_name, realm FROM characters')
    existing_chars = set(c.fetchall())
    conn.close()

    # Filter out members that are already in DB
    members_to_process = []
    for member in guild_info['members']:
        member_key = (member['name'].lower(), member['realm'])
        if member_key not in existing_chars:
            members_to_process.append(member)

    if not members_to_process:
        return jsonify({
            'success': True,
            'message': f'All {total_members} guild members are already in the database.'
        })

    processed = 0
    added = 0
    errors = 0

    # Process with concurrent requests - REMOVED THE [:100] LIMIT
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    # Rate limiting
    rate_limit_per_second = 70
    request_interval = 1 / rate_limit_per_second
    last_request_time = [time.time()]
    request_lock = threading.Lock()

    def process_member_with_rate_limit(member):
        nonlocal last_request_time, request_lock

        with request_lock:
            current_time = time.time()
            time_since_last = current_time - last_request_time[0]
            if time_since_last < request_interval:
                sleep(request_interval - time_since_last)
            last_request_time[0] = time.time()

        return process_single_member(member, access_token)

    # Use ThreadPoolExecutor for concurrent processing - PROCESS ALL MEMBERS
    with ThreadPoolExecutor(max_workers=50) as executor:
        future_to_member = {
            executor.submit(process_member_with_rate_limit, member): member
            for member in members_to_process  # REMOVED [:100] - now processes ALL members
        }

        for future in as_completed(future_to_member):
            try:
                result = future.result()
                if result['success']:
                    added += 1
                else:
                    errors += 1
                processed += 1
            except Exception as e:
                errors += 1
                processed += 1

    return jsonify({
        'success': True,
        'message': f'Guild search completed. Processed: {processed}/{len(members_to_process)}, Added: {added}, Errors: {errors}, Skipped (already in DB): {total_members - len(members_to_process)}'
    })

def process_single_member(member, access_token):
    """Process a single guild member"""
    try:
        member_name = member['name'].lower()
        member_realm = member['realm']

        # Fetch pet data
        _, _, pet_data = fetch_pet_data('eu', member_realm, member_name, access_token, timeout=10, retries=2)

        if pet_data == 'error_typing':
            return {'success': False, 'reason': 'character_not_found'}
        elif not pet_data:
            return {'success': False, 'reason': 'api_error'}

        # Process account association
        account_id_to_use = check_existing_pet_data(pet_data)
        if not account_id_to_use:
            account_id_to_use = fetch_next_account_id()

        # Insert character data
        insert_or_update_character_data(account_id_to_use, member_name, member_realm, pet_data)
        return {'success': True}

    except Exception as e:
        return {'success': False, 'reason': f'exception: {str(e)}'}

def get_character_avatar_threaded(character_name, realm, region="eu"):
    global avatar_last_request_time, avatar_request_lock

    # Apply rate limiting
    with avatar_request_lock:
        current_time = time.time()
        time_since_last = current_time - avatar_last_request_time[0]
        if time_since_last < avatar_request_interval:
            sleep(avatar_request_interval - time_since_last)
        avatar_last_request_time[0] = time.time()

    access_token = get_access_token()
    if not access_token:
        return None

    realm_slug = realm.lower()
    character_name_api = character_name.lower()

    url = f"https://{region}.api.blizzard.com/profile/wow/character/{realm_slug}/{character_name_api}/character-media?namespace=profile-{region}&locale=en_EU"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    try:
        response = requests.get(url, headers=headers, timeout=3)
        response.raise_for_status()
        character_media = response.json()

        if 'assets' in character_media:
            for asset in character_media['assets']:
                if asset.get('key') == 'avatar':
                    return asset['value']
            if character_media['assets']:
                return character_media['assets'][0]['value']
        return None
    except:
        return None

@app.route('/get_avatars_batch', methods=['POST'])
def get_avatars_batch():
    characters = request.json.get('characters', [])
    if not characters:
        return jsonify({'avatars': {}})

    access_token = get_access_token()
    if not access_token:
        return jsonify({'avatars': {}})

    def fetch_single_avatar(char_data):
        character_name = char_data['name']
        realm = char_data['realm']
        avatar_url = get_character_avatar_threaded(character_name, realm)
        return f"{character_name}-{realm}", avatar_url

    avatars = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_char = {
            executor.submit(fetch_single_avatar, char): char
            for char in characters
        }

        for future in as_completed(future_to_char):
            try:
                char_key, avatar_url = future.result(timeout=10)
                avatars[char_key] = avatar_url
            except:
                char = future_to_char[future]
                avatars[f"{char['name']}-{char['realm']}"] = None

    return jsonify({'avatars': avatars})

@app.route('/get_avatar/<character_name>/<realm>')
def get_avatar(character_name, realm):
    avatar_url = get_character_avatar_threaded(character_name, realm)

    if avatar_url is None:
        # Instead of removing, validate character via pet data
        access_token = get_access_token()
        if access_token:
            # Check if character exists by fetching pet data
            _, _, pet_data = fetch_pet_data('eu', realm, character_name, access_token)

            if pet_data == 'error_typing':
                # Character truly doesn't exist - remove from database
                try:
                    remove_character_from_database(character_name, realm)
                    return jsonify({
                        'avatar_url': None,
                        'removed': True,
                        'message': 'Character not found and removed from database'
                    })
                except Exception as e:
                    return jsonify({
                        'avatar_url': None,
                        'removed': False,
                        'message': 'Failed to remove character from database'
                    })
            elif pet_data:
                # Character exists but avatar unavailable - keep in database
                return jsonify({
                    'avatar_url': None,
                    'removed': False,
                    'valid_character': True,
                    'message': 'Character is valid but avatar unavailable'
                })

    return jsonify({
        'avatar_url': avatar_url,
        'removed': False,
        'valid_character': True if avatar_url else False
    })

def get_character_avatar(character_name, realm, region="eu"):
    access_token = get_access_token()
    if not access_token: # Added check
        return None

    realm_slug = realm.lower() # Assuming realm is already a slug or simple name

    # Ensure character_name is lowercase for the API URL
    # Character names are typically case-insensitive in WoW, but API paths might be strict or prefer lowercase
    character_name_api = character_name.lower()

    url = f"https://{region}.api.blizzard.com/profile/wow/character/{realm_slug}/{character_name_api}/character-media?namespace=profile-{region}&locale=en_EU"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    try: # Added try-except for robustness
        response = requests.get(url, headers=headers)
        response.raise_for_status() # Raise HTTPError for bad responses (4XX or 5XX)
        character_media = response.json()

        # Iterate through assets to find the avatar, typically the first one or one with key 'avatar'
        if 'assets' in character_media:
            for asset in character_media['assets']:
                if asset.get('key') == 'avatar':
                    return asset['value']
            if character_media['assets']: # Fallback to first asset if no 'avatar' key found
                 return character_media['assets'][0]['value']
        # Some responses might directly have an "avatar_url" field at the top level (less common for this endpoint)
        # if 'avatar_url' in character_media:
        # return character_media['avatar_url']
        return None # No suitable avatar found
    except requests.exceptions.RequestException:
        return None
    except (KeyError, IndexError):
        return None


# Function to remove character from the database
def remove_character_from_database(character_name, realm):
    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    # Ensure names are matched case-insensitively if necessary, or ensure stored names are consistent
    # Assuming character_name and realm in DB are stored consistently (e.g., lowercase)
    c.execute('''DELETE FROM characters WHERE lower(character_name) = ? AND lower(realm) = ?''', (character_name.lower(), realm.lower()))
    conn.commit()
    conn.close()

# Database initialization
def initialize_database():
    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS characters (
                id INTEGER PRIMARY KEY,
                account_id INTEGER,
                character_name TEXT,
                realm TEXT,
                pet_data TEXT,
                UNIQUE(character_name, realm)
                )''')
    conn.commit()
    conn.close()

initialize_database()

# Function to get access token for Blizzard API
def get_access_token():
    url = 'https://us.battle.net/oauth/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
    }
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()
        return response.json()['access_token']
    except requests.exceptions.RequestException:
        return None
    except KeyError: # If 'access_token' is not in the response
        return None

# Function to fetch pet data
def fetch_pet_data(region, realm, character_name, access_token, timeout=5, retries=3):
    character_name_api = character_name.lower()
    realm_api = realm.lower().replace(' ', '-')
    character_name_encoded = quote(character_name_api)

    base_url = f'https://{region}.api.blizzard.com/profile/wow/character/{realm_api}/{character_name_encoded}/collections/pets'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Battlenet-Namespace': f'profile-{region}',
        'locale':'en_US'
    }

    for attempt in range(retries):
        try:
            response = requests.get(base_url, headers=headers, timeout=timeout)
            response.raise_for_status()
            full_data = response.json()
            pet_collection = full_data.get("pets", [])

            # More efficient sorting for bulk operations
            if pet_collection:
                sorted_pets = sorted(pet_collection, key=lambda p: p.get('id', 0))
            else:
                sorted_pets = []

            pet_data_json = json.dumps(sorted_pets)
            pets_data_bytes = pet_data_json.encode('utf-8')
            sha256_hash = hashlib.sha256()
            sha256_hash.update(pets_data_bytes)
            petsback = sha256_hash.digest()
            pets_data_hash = base64.b64encode(petsback).decode('utf-8')

            return character_name, realm, pets_data_hash

        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 404:
                return character_name, realm, 'error_typing'
            elif http_err.response.status_code == 429:  # Rate limited
                if attempt < retries - 1:
                    sleep(2 ** attempt)  # Exponential backoff
                    continue
            return character_name, realm, None

        except requests.exceptions.RequestException:
            if attempt < retries - 1:
                sleep(1 + attempt)
                continue
            else:
                return character_name, realm, None
        except (KeyError, json.JSONDecodeError):
             return character_name, realm, None

    return character_name, realm, None


# Function to insert or update character data into database
def insert_or_update_character_data(account_id, character_name, realm, pet_data):
    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()

    # Store character_name and realm in a consistent format (e.g., lowercase)
    db_character_name = character_name.lower()
    db_realm = realm.lower().replace(' ', '-') # Store as slug

    c.execute('''SELECT id FROM characters WHERE character_name = ? AND realm = ?''', (db_character_name, db_realm))
    existing_character = c.fetchone()

    if existing_character:
        c.execute('''UPDATE characters SET account_id = ?, pet_data = ? WHERE id = ?''',
                  (account_id, pet_data, existing_character[0]))
    else:
        c.execute('''INSERT INTO characters (account_id, character_name, realm, pet_data) VALUES (?, ?, ?, ?)''',
                  (account_id, db_character_name, db_realm, pet_data))
    conn.commit()
    conn.close()

# Function to check if pet data already exists in the database
def check_existing_pet_data(pet_data):
    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    c.execute('''SELECT account_id FROM characters WHERE pet_data = ?''', (pet_data,)) # Removed one pet_data from tuple
    existing_data = c.fetchall() # Use fetchall if multiple accounts could somehow have same hash
    conn.close()
    if existing_data:
        return existing_data[0][0] # Return first found account_id
    return None


# Function to fetch next available account ID
def fetch_next_account_id():
    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    c.execute('''SELECT MAX(account_id) FROM characters''')
    max_account_id = c.fetchone()[0]
    conn.close()
    return (max_account_id + 1) if max_account_id is not None else 1


# Function to merge accounts
def merge_accounts(account_id_to_keep, account_id_to_merge):
    if account_id_to_keep == account_id_to_merge: return # No need to merge same accounts

    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    c.execute('''UPDATE characters SET account_id = ? WHERE account_id = ?''', (account_id_to_keep, account_id_to_merge))
    conn.commit()
    conn.close()

# Route to handle the form submission
@app.route('/submit', methods=['POST', 'GET'])
def submit_character():
    character_name_input = ""
    realm_input = ""

    if request.method == 'GET':
        character_name_input = request.args.get('character_name', '').strip()
        realm_input = request.args.get('realm', '').strip()
    else:  # POST
        character_name_input = request.form.get('character_name', '').strip().lower()
        realm_input = request.form.get('realm', '').strip().lower().replace(' ', '-')

    if not character_name_input or not realm_input:
        return render_template('form.html', error='Character name and realm are required.')

    # Use consistent casing for operations but can retain original for display if needed
    # For API calls and DB, use lowercase character name and slugified realm
    character_name_processed = character_name_input.lower()
    realm_processed = realm_input.lower().replace(' ', '-')


    region = 'eu' # Assuming EU, could be made dynamic
    access_token = get_access_token()
    if not access_token:
        return render_template('form.html', error='Failed to get access token. Please try again later.', accounts={})

    # Pass processed names to fetch_pet_data
    _, _, pet_data = fetch_pet_data(region, realm_processed, character_name_processed, access_token)

    # Pass processed names for guild data as well
    guild_info = fetch_guild_data(character_name_processed, realm_processed, access_token)


    if pet_data == 'error_typing':
        # This means character was not found (404) by fetch_pet_data
        # remove_character_from_database is already called within fetch_pet_data for 404s
        return render_template('form.html', error='Character not found or API access issue. If this character was in the database, it has been removed.', accounts={}, character_name=character_name_input, realm=realm_input)

    if not pet_data: # General failure to fetch pet data
         return render_template('form.html', error='Failed to fetch character pet data. Please check name/realm or try again.', accounts={}, character_name=character_name_input, realm=realm_input)


    account_id_to_use = None
    existing_account_id_for_pets = check_existing_pet_data(pet_data)

    if existing_account_id_for_pets:
        account_id_to_use = existing_account_id_for_pets
    else:
        account_id_to_use = fetch_next_account_id()

    # Check if the *current* character (name/realm) is already in DB and under which account
    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    c.execute('SELECT account_id FROM characters WHERE character_name = ? AND realm = ?', (character_name_processed, realm_processed))
    result = c.fetchone()
    conn.close()

    if result:
        current_db_account_id = result[0]
        if current_db_account_id != account_id_to_use:
            # This character exists but its pet data hash has changed or it was under a different account_id that needs merging
            # Or, this character's pet data matches an existing account (account_id_to_use), and its old account_id (current_db_account_id)
            # should be merged into account_id_to_use
            merge_accounts(account_id_to_use, current_db_account_id)
            # All characters from current_db_account_id are now part of account_id_to_use

    # Insert/Update the current character with the determined account_id_to_use
    insert_or_update_character_data(account_id_to_use, character_name_processed, realm_processed, pet_data)

    # Fetch all characters associated with this account_id_to_use
    all_chars_for_account = get_account_characters(account_id_to_use)

    characters_data_display = [{'character_name': char[0], 'realm': char[1]} for char in all_chars_for_account]
    # Sort characters for consistent display, e.g., by name then realm
    characters_data_display.sort(key=lambda x: (x['character_name'], x['realm']))


    accounts_display = {account_id_to_use: characters_data_display}

    return render_template('form.html', accounts=accounts_display, guild_info=guild_info, character_name=character_name_input, realm=realm_input)


# Function to normalize names by removing special characters for suggestion matching
def normalize_name(name):
    normalized_name = unidecode(name) # Transliterate (e.g., 'č' to 'c')
    normalized_name = re.sub(r'[^a-zA-Z0-9]', '', normalized_name).lower() # Remove non-alphanumeric
    return normalized_name

# Replace your current search_guild_member route with this:
@app.route('/search_guild_member')
def search_guild_member():
    character_name_query = request.args.get('character_name', '').strip()
    realm_query = request.args.get('realm', '').strip() # This should be a slug from the JS

    if not character_name_query or not realm_query:
        return render_template('form.html', error='Character name and realm are required for guild member search.', accounts={})

    # Normalize for API calls: character name to lower, realm to slug (already should be from JS)
    character_name_api = character_name_query.lower()
    realm_api = realm_query.lower().replace(' ', '-') # Ensure it's a slug

    access_token = get_access_token()
    if not access_token:
        return render_template('form.html', error='Failed to get access token. Please try again later.', accounts={})

    # Fetch pet data for the clicked guild member
    _, _, pet_data = fetch_pet_data('eu', realm_api, character_name_api, access_token)

    # Fetch guild data for the original character (or the one being focused on)
    # This route is for when a guild member is clicked. So character_name_api is the target.
    guild_info = fetch_guild_data(character_name_api, realm_api, access_token)


    if pet_data == 'error_typing': # Character not found
        return render_template('form.html', error=f'Character {character_name_query} on {realm_query} not found or API access issue. If previously stored, it has been removed.', accounts={}, character_name=character_name_query, realm=realm_query)

    if not pet_data: # Other error fetching pet data
        return render_template('form.html', error=f'Failed to fetch pet data for {character_name_query} - {realm_query}. Please try again.', accounts={}, character_name=character_name_query, realm=realm_query)

    # Logic for account association based on pet_data (same as /submit)
    account_id_to_use = check_existing_pet_data(pet_data)
    if not account_id_to_use:
        account_id_to_use = fetch_next_account_id()

    # Check if this character (character_name_api, realm_api) has an existing DB entry
    # and merge if its old account_id differs from the account_id_to_use determined by pet_data
    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    c.execute('SELECT account_id FROM characters WHERE character_name = ? AND realm = ?', (character_name_api, realm_api))
    result = c.fetchone()
    conn.close()

    if result:
        current_db_account_id = result[0]
        if current_db_account_id != account_id_to_use:
            merge_accounts(account_id_to_use, current_db_account_id)

    insert_or_update_character_data(account_id_to_use, character_name_api, realm_api, pet_data)

    all_chars_for_account = get_account_characters(account_id_to_use)
    characters_data_display = [{'character_name': char[0], 'realm': char[1]} for char in all_chars_for_account]
    characters_data_display.sort(key=lambda x: (x['character_name'], x['realm']))

    accounts_display = {account_id_to_use: characters_data_display}

    # Pass original query params back to pre-fill form if desired
    return render_template('form.html', accounts=accounts_display, guild_info=guild_info, character_name=character_name_query, realm=realm_query)


# Route to get name suggestions
@app.route('/suggest_names', methods=['GET'])
def suggest_names():
    query = request.args.get('query', '').strip() # Strip whitespace
    if not query or len(query) < 2: # Require at least 2 chars for suggestions
        return jsonify([])

    normalized_query = normalize_name(query) # Uses unidecode and removes special chars

    conn = sqlite3.connect('/home/tigercz11/blizzard_accounts.db')
    c = conn.cursor()
    # Fetch all distinct character_name, realm pairs
    c.execute('SELECT DISTINCT character_name, realm FROM characters')
    all_names = c.fetchall()
    conn.close()

    suggestions = []
    for name, realm in all_names:
        # Normalize DB names on-the-fly for matching
        if normalized_query in normalize_name(name):
            suggestions.append({'name': name, 'realm': realm})
            if len(suggestions) >= 20: # Limit suggestions
                break

    return jsonify(suggestions)

# Route to display form
@app.route('/')
def form():
    # Get query parameters to pre-fill form if navigating back or sharing a link
    character_name = request.args.get('character_name', '')
    realm = request.args.get('realm', '')
    return render_template('form.html', accounts={}, character_name=character_name, realm=realm)

if __name__ == '__main__':
    initialize_database() # Ensure DB is initialized on startup
    app.run(debug=True)
