from os import  getenv as env
from pathlib import Path
from dataclasses import dataclass, fields
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Directories:
    '''
    Contains Project level directories as path objects
    '''
    root = Path(env('ROOT_DIR','./')).resolve()

    base = root / 'src' / 'app'
    logs = root / 'logs'
    data = root / 'data'

    def __repr__(self) -> str:
        return f"""root={self.root}\nbase={self.base}\nlogs={self.logs},\ndata={self.data}"""


dirs = Directories()

for i in fields(dirs):
    print(i)
    # if not i[0].startswith('__'):
    #     i[1].mkdir(parents=True, exist_ok=True)
