import os
import json
import shutil
from datetime import datetime
import dotenv
dotenv.load_dotenv()
METADATA_PATH = os.getenv("METADATA_PATH")

def archive_and_update_metadata(st,metadata_file, metadata_json):
        # Archivia il file precedente
        archive_dir = os.path.join(METADATA_PATH, "archived_metadata")
        os.makedirs(archive_dir, exist_ok=True)
        if os.path.exists(metadata_file):
            archived_name = os.path.basename(metadata_file)
            archived_path = os.path.join(archive_dir, archived_name)
            shutil.copy2(metadata_file, archived_path)

        # Aggiorna il timestamp nel nome file
        base_name = f"{st.session_state.selected_version}__{st.session_state.selected_dataset_name}__{st.session_state.selected_subpath}__{datetime.now().strftime('%Y%m%d%H%M')}.json"
        new_metadata_file = os.path.join(METADATA_PATH, base_name)

        # Salva il nuovo file
        with open(new_metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata_json, f, indent=2, ensure_ascii=False)

        return new_metadata_file



