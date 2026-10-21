import ast
import base64
import hashlib
import importlib.metadata
import json
from pathlib import Path
from zipfile import ZipFile

dist = importlib.metadata.distribution('durable-workflow')
assert dist.version == '2.6.2'
wheel = Path('/phase/sdk.whl')
assert hashlib.sha256(wheel.read_bytes()).hexdigest() == '28379c16bcd598c9aa545071d074bee297e449158a550e580d1df0ef9fc27ab8'
archive = ZipFile(wheel)
checked = 0
for entry in dist.files or []:
    if not str(entry).startswith('durable_workflow/') or entry.hash is None:
        continue
    path = dist.locate_file(entry)
    assert str(path).startswith('/opt/venv/'), path
    expected = entry.hash.value.rstrip('=')
    actual = base64.urlsafe_b64encode(hashlib.sha256(path.read_bytes()).digest()).decode().rstrip('=')
    assert expected == actual, str(entry)
    assert path.read_bytes() == archive.read(str(entry)), str(entry)
    checked += 1
assert checked > 25
source = Path('/phase/consumer.py').read_text()
tree = ast.parse(source)
constants = {}
for node in tree.body:
    if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
        for target in node.targets:
            if isinstance(target, ast.Name):
                constants[target.id] = node.value.value
for wait in ('', constants['WAIT']):
    definition = constants['BASE'].format(body=constants['BODY_MARKER'] + wait + constants['RETURN'])
    compile(definition, '<actual-generated-workflow>', 'exec')
archive.close()
print(json.dumps({'installed_version': dist.version, 'production_record_and_wheel_hashes_checked': checked, 'source_override': False, 'generated_workflows_compile': True}))
