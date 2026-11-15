import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

PROGRAM = Path(__file__).resolve().parents[1] / 'ibx'


class InboxTests(unittest.TestCase):
    def setUp(self):
        self.area = tempfile.TemporaryDirectory()
        self.addCleanup(self.area.cleanup)
        self.box = Path(self.area.name) / 'box'

    def command(self, *args):
        return [sys.executable, str(PROGRAM), '--box', str(self.box), *args]

    def run_cli(self, *args, body=None, code=0):
        result = subprocess.run(self.command(*args), input=body, capture_output=True)
        self.assertEqual(result.returncode, code, result.stderr)
        return result

    def put(self, body=b'build finished', ident='item'):
        return self.run_cli('put', '--id', ident, body=body)

    def test_invalid_ids(self):
        for ident in ('../x', '.hidden', '', 'x' * 129):
            with self.subTest(ident=ident):
                self.run_cli('put', '--id', ident, 'body', code=2)

    def test_body_validation(self):
        self.run_cli('put', body=b'', code=2)
        self.run_cli('put', '--max-size', '3', 'four', code=2)
        self.run_cli('put', body=b'x' * (1024 * 1024 + 1), code=2)

    def test_duplicate_and_hash(self):
        import hashlib
        body = b'build finished'
        result = self.run_cli('put', body=body)
        self.assertEqual(result.stdout.strip().decode(), hashlib.sha256(body).hexdigest()[:16])
        self.put(ident='same')
        result = self.put(b'changed', 'same')
        self.assertIn(b'duplicate', result.stderr)
        self.assertEqual((self.box / 'pending/same.msg').read_bytes(), body)
        self.assertEqual(len(list((self.box / 'pending').iterdir())), 2)

    def test_concurrent_put(self):
        processes = [subprocess.Popen(self.command('put', '--id', 'same', 'body'), stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(20)]
        for process in processes:
            out, err = process.communicate(timeout=20)
            self.assertEqual(process.returncode, 0, err)
            self.assertEqual(out, b'same\n')
        self.assertEqual(len(list((self.box / 'pending').iterdir())), 1)

    def test_concurrent_take(self):
        for index in range(200):
            self.put(ident='item' + str(index))
        processes = [subprocess.Popen(self.command('--format', 'json', 'take'), stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(20)]
        ids = []
        for process in processes:
            out, err = process.communicate(timeout=20)
            self.assertEqual(process.returncode, 0, err)
            ids.extend(json.loads(line)['id'] for line in out.splitlines())
        self.assertEqual(len(ids), 200)
        self.assertEqual(set(ids), {'item' + str(i) for i in range(200)})

    def test_killed_after_tmp_write(self):
        script = '''import importlib.machinery, importlib.util, os, signal, sys
loader = importlib.machinery.SourceFileLoader('inbox', sys.argv[1])
spec = importlib.util.spec_from_loader(loader.name, loader)
module = importlib.util.module_from_spec(spec)
loader.exec_module(module)
def stopped(*args, **kwargs):
    os.kill(os.getpid(), signal.SIGSTOP)
module.os.link = stopped
module.main(['--box', sys.argv[2], 'put', 'body'])
'''
        process = subprocess.Popen([sys.executable, '-c', script, str(PROGRAM), str(self.box)])
        try:
            _, status = os.waitpid(process.pid, os.WUNTRACED)
            self.assertTrue(os.WIFSTOPPED(status))
            self.assertEqual(list((self.box / 'pending').iterdir()), [])
            self.assertEqual(len(list((self.box / 'tmp').iterdir())), 1)
        finally:
            process.kill()
            process.wait()

    def test_round_trips(self):
        body = b'line one\nline\ttwo\\end\r'
        for fmt in ('tsv', 'json'):
            self.put(body)
            output = self.run_cli('--format', fmt, 'take').stdout.decode()
            if fmt == 'json':
                self.assertEqual(json.loads(output), {'id': 'item', 'body': body.decode()})
            else:
                self.assertEqual(output, 'item\tline one\\nline\\ttwo\\\\end\\r\n')

    def test_list_take_max_since_and_order(self):
        self.put(ident='second')
        self.put(ident='first')
        for ident in ('first', 'second'):
            os.utime(self.box / ('pending/' + ident + '.msg'), ns=(1000000000, 1000000000))
        self.assertEqual(self.run_cli('list').stdout.splitlines()[0].split(b'\t')[0], b'first')
        self.assertEqual(len(list((self.box / 'pending').iterdir())), 2)
        self.assertEqual(self.run_cli('take', '--max', '1').stdout, b'first\tbuild finished\n')
        self.assertIn(b'first', self.run_cli('list', '--done', '--since', '10m').stdout)
        self.assertEqual(self.run_cli('list', '--done', '--since', '0').stdout, b'')
        self.run_cli('take')
        self.assertEqual(self.run_cli('take').stdout, b'')

    def test_wait_timeout_and_arrival(self):
        self.assertEqual(self.run_cli('wait', '--timeout', '.05', '--poll', '.01', code=4).stdout, b'')
        process = subprocess.Popen(self.command('wait', '--timeout', '5', '--poll', '.01'), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.put()
        out, err = process.communicate(timeout=10)
        self.assertEqual(process.returncode, 0, err)
        self.assertEqual(out, b'item\tbuild finished\n')

    def test_stream(self):
        import select
        process = subprocess.Popen(self.command('stream', '--poll', '.01'), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            self.put(ident='first')
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            self.assertEqual(process.stdout.readline(), b'first\tbuild finished\n')
            self.put(ident='second')
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            self.assertEqual(process.stdout.readline(), b'second\tbuild finished\n')
            process.stdout.close()
            self.put(ident='third')
            process.wait(timeout=5)
            self.assertEqual(process.returncode, 0)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stderr.close()

    def notification(self, *extra, ident='item', code=0):
        return self.run_cli('send', '--id', ident, '--notify-cmd', 'printf "hint\\n" >> "$IBX_BOX/hints"', *extra, 'body', code=code)

    def test_send_success_and_coalescing(self):
        self.notification()
        self.notification(ident='second')
        self.assertEqual((self.box / 'hints').read_text(), 'hint\n')
        self.run_cli('take')
        self.notification(ident='third')
        self.assertEqual((self.box / 'hints').read_text(), 'hint\nhint\n')

    def test_send_failure(self):
        result = self.run_cli('send', '--notify-cmd', 'exit 1', 'body', code=3)
        self.assertIn(b'warning', result.stderr)
        self.assertEqual(len(list((self.box / 'pending').iterdir())), 1)
        self.assertFalse((self.box / 'state/notified').exists())

    def test_send_stale(self):
        self.notification()
        os.utime(self.box / 'state/notified', (1, 1))
        self.notification('--renotify-after', '1s', ident='second')
        self.assertEqual((self.box / 'hints').read_text(), 'hint\nhint\n')

    def test_send_environment(self):
        secret = 'private body text'
        self.run_cli('send', '--id', 'item', '--notify-cmd', 'env > "$IBX_BOX/environment"', secret)
        env = (self.box / 'environment').read_text()
        self.assertNotIn(secret, env)
        self.assertIn('IBX_ID=item\n', env)
        self.assertIn('IBX_PENDING=1\n', env)
        self.assertIn('IBX_BOX=' + str(self.box), env)

    def test_send_timeout_and_no_command(self):
        self.run_cli('send', '--notify-cmd', 'sleep 1', '--notify-timeout', '.02', 'body', code=3)
        self.run_cli('send', '--notify-cmd', '', 'other')
        self.assertEqual(len(list((self.box / 'pending').iterdir())), 2)

    def test_gc(self):
        # gc ages done/ files by change time (when they were taken), not by
        # modification time: a message that waited long in pending but was
        # taken just now must survive.
        self.put(ident='stale')
        os.utime(self.box / 'pending/stale.msg', (1, 1))
        self.run_cli('take')
        self.put(ident='pending')
        for name in ('done/old.msg', 'tmp/old', 'tmp/new', 'state/other'):
            (self.box / name).write_bytes(b'body')
        for name in ('tmp/old', 'pending/pending.msg', 'state/other'):
            os.utime(self.box / name, (1, 1))
        self.assertEqual(self.run_cli('gc', '--keep', '1h').stdout, b'1\n')
        for name in ('done/stale.msg', 'done/old.msg', 'tmp/new', 'pending/pending.msg', 'state/other'):
            self.assertTrue((self.box / name).exists(), name)
        self.assertFalse((self.box / 'tmp/old').exists())
        # With a zero keep period, done/ files taken in the past are removed.
        # tmp/ always uses its own one-hour threshold, and pending/ and
        # state/ are never touched.
        self.assertEqual(self.run_cli('gc', '--keep', '0').stdout, b'2\n')
        for name in ('done/stale.msg', 'done/old.msg'):
            self.assertFalse((self.box / name).exists(), name)
        for name in ('tmp/new', 'pending/pending.msg', 'state/other'):
            self.assertTrue((self.box / name).exists(), name)

    def test_permissions_bytes_and_reuse(self):
        self.put(b'raw\xff')
        self.assertEqual((self.box / 'pending/item.msg').read_bytes(), b'raw\xff')
        for name in ('', 'tmp', 'pending', 'done', 'state'):
            self.assertEqual((self.box / name).stat().st_mode & 0o777, 0o700)
        self.assertEqual((self.box / 'pending/item.msg').stat().st_mode & 0o777, 0o600)
        self.assertIn(b'\xef\xbf\xbd', self.run_cli('take').stdout)
        self.put(b'again')
        self.assertEqual(self.run_cli('take').stdout, b'item\tagain\n')

    def test_missing_box(self):
        env = os.environ.copy()
        env.pop('IBX_BOX', None)
        result = subprocess.run([sys.executable, str(PROGRAM), 'list'], env=env, capture_output=True)
        self.assertEqual(result.returncode, 2)


if __name__ == '__main__':
    unittest.main()
