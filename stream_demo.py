"""Really consume an Arrow C stream: read rows, keep a shared slice alive
across Reader.close(), then show the producer-side release records."""
import json

from stream_fixture import Source
from stream_import import open_stream

EXPECTED = [
    {'id': 10, 'tag': '蓝', 'samples': [5, 6]},
    None,
    {'id': 30, 'tag': None, 'samples': [7]},
]


def array_root_released(bundle):
    return any(e['kind'] == 'array' and e['node'] == 'root' for e in bundle.events)


def main():
    source = Source(count=2)
    reader = open_stream(source.pointer)

    first = reader.next()
    rows = [first.row(i) for i in range(first.length)]
    kept = first.slice(1, 2)        # shared-ownership view of rows 1..2
    deepest = kept.slice(1, 1)      # nested slice of the last row

    second = reader.next()
    second_rows = [second.row(i) for i in range(second.length)]

    ended = reader.next() is None   # normal end closes the stream
    reader.close()
    first.close()
    second.close()
    # The array root of batch 1 is still owned by the open slices.
    alive_after_reader_close = not array_root_released(source.bundles[0])

    # The kept slices are still fully readable after Reader and siblings closed.
    kept_rows = [kept.row(i) for i in range(kept.length)]
    deepest_row = deepest.row(0)
    deepest.close()
    kept.close()                    # last view of batch 1: root released now
    released_after_last_view = array_root_released(source.bundles[0])

    report = {
        'rows': rows,
        'second_rows': second_rows,
        'ended': ended,
        'kept_rows_after_reader_close': kept_rows,
        'deepest_row': deepest_row,
        'array_alive_after_reader_close': alive_after_reader_close,
        'array_released_after_last_view_close': released_after_last_view,
        'stream_events': source.events,
        'batch1_release_record': source.bundles[0].events,
        'batch2_release_record': source.bundles[1].events,
        'schema_release_record': source.schemas[0].events,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))

    assert rows == EXPECTED and second_rows == EXPECTED
    assert kept_rows == EXPECTED[1:] and deepest_row == EXPECTED[2]
    assert ended and alive_after_reader_close and released_after_last_view
    for bundle in source.bundles:
        assert sum(e['kind'] == 'array' and e['node'] == 'root'
                   for e in bundle.events) == 1
    assert sum(e['kind'] == 'schema' and e['node'] == 'root'
               for e in source.schemas[0].events) == 1
    assert sum(1 for e in source.events if e[0] == 'release') == 1
    assert not any(e['outside_parent'] for b in source.bundles + source.schemas
                   for e in b.events)
    print('STREAM_DEMO_OK')


if __name__ == '__main__':
    main()
