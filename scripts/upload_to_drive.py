import os
import pickle
import mimetypes
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.file']

CLIENT_SECRETS_FILE = os.getenv('CLIENT_SECRETS_FILE', 'client_secrets.json')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
MAKE_PUBLIC = os.getenv('MAKE_PUBLIC', 'false').lower() == 'true'

def get_drive_service():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CLIENT_SECRETS_FILE):
                raise FileNotFoundError(f"Client secrets file '{CLIENT_SECRETS_FILE}' not found. Please download it from Google Cloud Console.")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

def create_folder(service, name, parent_id=None):
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        file_metadata['parents'] = [parent_id]
    
    file = service.files().create(body=file_metadata, fields='id').execute()
    return file.get('id')

def find_folder(service, name, parent_id=None):
    query = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    if items:
        return items[0]['id']
    return None

def upload_file(service, filename, filepath, folder_id):
    # Check if file exists
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    items = results.get('files', [])
    
    if items:
        print(f"Skipping {filename}: Already exists (ID: {items[0]['id']})")
        return items[0]['id']

    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    media = MediaFileUpload(filepath, mimetype=mimetypes.guess_type(filepath)[0])
    
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"Uploaded {filename} (ID: {file.get('id')})")
    
    if MAKE_PUBLIC:
        permission = {
            'type': 'anyone',
            'role': 'reader',
        }
        service.permissions().create(fileId=file.get('id'), body=permission).execute()
        
    return file.get('id')

def main():
    service = get_drive_service()
    
    folder_id = DRIVE_FOLDER_ID
    if not folder_id:
        print("DRIVE_FOLDER_ID not set in .env. Checking for 'Colab_Notebooks' folder...")
        folder_name = "Colab_Notebooks"
        folder_id = find_folder(service, folder_name)
        if not folder_id:
            print(f"Creating folder '{folder_name}'...")
            folder_id = create_folder(service, folder_name)
        print(f"Using Folder ID: {folder_id}")

    notebooks_dir = 'notebooks'
    if not os.path.exists(notebooks_dir):
        print(f"Directory '{notebooks_dir}' not found.")
        return

    print(f"Scanning '{notebooks_dir}' for notebooks...")
    count = 0
    for root, dirs, files in os.walk(notebooks_dir):
        for file in files:
            if file.endswith('.ipynb'):
                filepath = os.path.join(root, file)
                upload_file(service, file, filepath, folder_id)
                count += 1
    
    print(f"Finished processing {count} notebooks.")

if __name__ == '__main__':
    main()
