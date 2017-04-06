#perl

use btindex;

#use LWP;
#use HTTP::Request::Common;
use HTTP::Tiny;
use JSON::PP;

use threads;
use Thread::Queue;

use strict;

$| = 1;

warn time;
use IO::Socket::SSL; IO::Socket::SSL->VERSION(1.42);
warn time;

my $webapi = btindex::config('webapi') || die 'setup webapi in config';

my $qc = new Thread::Queue();
my $qa = new Thread::Queue();

$qc->limit = 1000;
$qa->limit = 10;


my @ta;
for(1..3) { push(@ta, 
  threads->create(sub
  {
    my ($id) = @_;
  
    print("thread add $id start...\n");

    #my $ua = new LWP::UserAgent();
    #$ua->timeout(10);
    my $ua = new HTTP::Tiny();

    while(defined(my $hash = $qa->dequeue()))
    {
      my $tf = btindex::torrent_path($hash);
      my $data = btindex::read_file($tf);
      my @s = stat($tf);
      #my $req = HTTP::Request::Common::POST(
      #    $webapi.'/add/?time='.$s[9],
      #    'Content-Type' => 'application/octet-stream',
      #    'Content-Length' => length($data),
      #    Content => $data
      #  );

      #my $res = $ua->request($req);
      
      my $res = $ua->post($webapi.'/add/?time='.$s[9], {
          headers => {
            'Content-Type' => 'application/octet-stream',
            'Content-Length' => length($data),
          },
          content => $data
        });

      printf("add %d\t%s\t%s\n", $id, $hash, $res->{status});
    }

    print("thread add $id end...\n");
    return;
  }, $_))
}


my $tc = threads->create(sub
  {
    print("thread check start...\n");

    my $check = sub
    {
      my @hashs = @_;

      printf("check\t%s .. %s\n", substr($hashs[0], 0, 18), substr($hashs[-1], 0, 18));

      #my $ua = new LWP::UserAgent();
      #$ua->timeout(10);
      
      my $ua = new HTTP::Tiny();

      #my $req = HTTP::Request::Common::POST(
      #    $webapi.'/mcheck/',
      #    'Content-Type' => 'application/json',
      #    Content => encode_json(\@hashs));

      #my $res = $ua->request($req);
      my $res = $ua->post($webapi.'/mcheck/', { 
          headers => { 'Content-Type' => 'application/json' }, 
          content => JSON::PP::encode_json(\@hashs) 
        });

      if($res->{status} != 200)
      {
        printf("check\terror %d\t%s\n", $res->{status}, $res->{reason});
        return;
      }

      $res = JSON::PP::decode_json($res->{content});
      my $add = 0;
      while(@$res)
      {
        my $hash = shift(@hashs);
        my $r = shift(@$res);
        #printf("check\t%s\t%s\n", $hash, $r);
        if($r == 404)
        {
          $qa->enqueue($hash);
          $add++;
        }
      }
      if($add) { printf("check\t%s .. %s\t%d\n", substr($_[0], 0, 18), substr($_[-1], 0, 18), $add); }
    };

    my @hashs;
    while(defined(my $hash = $qc->dequeue()))
    {
      #printf("check\t%s\t%d\t%d\n", $hash, scalar(@hashs), $qc->limit());
      push(@hashs, $hash);
      if(scalar(@hashs) == $qc->limit()) { $check->(@hashs); @hashs = (); }
    }
    if(@hashs) { $check->(@hashs); }
    #$qa->enqueue(undef) foreach(@ta);
    $qa->end();

    print("thread check end...\n");
    #$_->join() foreach(@ta);
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

    #$qc->enqueue(undef);
    $qc->end();
    
    #$tc->join();
    #threads->detach();
  });

#$ts->detach();


while(grep { $_->is_running() } threads->list()) 
{ 
  printf("running... %d/%d\n", $qc->pending(), $qa->pending());
  sleep(1); 
}

$ts->join();
$tc->join();
foreach(@ta) { $_->join(); }

print("all done...\n");

exit 0;
