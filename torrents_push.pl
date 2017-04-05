#perl

use btindex;

use LWP;
use HTTP::Request::Common;
use JSON::PP;

use threads;
use Thread::Queue;

use strict;


my $webapi = btindex::config('webapi') || die 'setup webapi in config';

my $qc = new Thread::Queue();
my $qa = new Thread::Queue();

$qc->limit = 1000;
$qa->limit = 10;


my $ta = threads->create(sub
  {
    print("thread add start...\n");

    my $ua = new LWP::UserAgent();
    $ua->timeout(10);

    while(defined(my $tid = $qa->dequeue()))
    {
      my $tf = btindex::torrent_path($tid);
      my $data = read_file($tf);
      my @s = stat($tf);
      my $req = HTTP::Request::Common::POST(
          $webapi.'/add/?time='.$s[9],
          'Content-Type' => 'application/octet-stream',
          'Content-Length' => length($data),
          Content => $data
        );

      my $res = $ua->request($req);

      printf("add\t%s\t%s\n", $tid, $res->code());
    }

    print("thread add end...\n");
    return;
  });


my $tc = threads->create(sub
  {
    print("thread check start...\n");

    my $check = sub
    {
      my @ids = @_;

      my $ua = new LWP::UserAgent();
      $ua->timeout(10);

      my $req = HTTP::Request::Common::POST(
          $webapi.'/mcheck/',
          'Content-Type' => 'application/json',
          Content => encode_json(\@ids));

      my $res = $ua->request($req);

      printf("check\t%s .. %s\n", substr($ids[0], 0, 18), substr($ids[-1], 0, 18));
      if($res->code() != 200)
      {
        printf("check\terror %d\n", $res->code());
        return;
      }

      $res = decode_json($res->decoded_content());
      my $add = 0;
      while(@$res)
      {
        my $id = shift(@ids);
        my $r = shift(@$res);
        #printf("check\t%s\t%s\n", $id, $r);
        if($r == 404)
        {
          $qa->enqueue($id);
          $add++;
        }
      }
      if($add) { printf("check\t%s .. %s\t%d\n", substr($_[0], 0, 18), substr($_[-1], 0, 18), $add); }
    };

    my @ids;
    while(defined(my $tid = $qc->dequeue()))
    {
      #printf("check\t%s\t%d\t%d\n", $tid, scalar(@ids), $qc->limit());
      push(@ids, $tid);
      if(scalar(@ids) == $qc->limit()) { $check->(@ids); @ids = (); }
    }
    if(@ids) { $check->(@ids); }
    $qa->enqueue(undef);

    print("thread check end...\n");
    $ta->join();
    return;
  });


my $ts = threads->create(sub
  {
    print("thread scan start...\n");
    foreach_torrent(
      start => uc($ARGV[0]),
      sub
      {
        my ($tf, $data) = @_;

        my @tf = split('/', $tf);

        #printf("scan\t%s\t%d\n", $tf[-1], $qc->pending());
        $qc->enqueue($tf[-1]);

        return;
      });
    print("thread scan end...\n");

    $qc->enqueue(undef);
    $tc->join();
    return;
  });

$ts->join();

undef $qc;
undef $qa;

while(grep { $_->is_running() } threads->list()) { sleep(1); }

print("all done...\n");
