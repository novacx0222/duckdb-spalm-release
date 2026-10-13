from pathlib import Path
import json,re
root=Path(__file__).resolve().parents[1]
# Every preexisting library timing site still exists with a renamed label.
old=json.loads((root/'docs/timing-library-mapping.json').read_text())
for item in old:
    text=(root/item['file']).read_text()
    if item['file'].endswith('ss_mat.h'):
        assert '-cumulative]' in text
    else:
        assert item['new'] in text,item
text=(root/'src/physical_spalm.cpp').read_text()
for side in [1,2]:
    for label in ['collect-input','combine-input','dimension-scan','csr-allocate','scan-chunks-prepare','prepare-total']:
        assert f'{side}-{label}' in text,(side,label)
assert 'reduction(+:extraction_ms)' in text
for label in ['result-allocate-zero-total','output-total','prepare-total','library-call-total','wrapper-deallocate']:
    assert label in text,label
for p in list((root/'spalm-release/src').rglob('*.cpp'))+list((root/'spalm-release/include').rglob('*.h')):
    for match in re.finditer(r'print_time\("([^"\n]+)"',p.read_text()):
        assert match[1].startswith('[') and match[1].endswith(']'),(p,match[1])
print('Timing labels, original-node coverage and worker-sum contract passed')
