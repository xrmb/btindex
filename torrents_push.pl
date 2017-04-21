#!perl

use threads;
use Thread::Queue;

use JSON::PP;
use Win32::Console;
use Cwd;

use btindex;

use strict;

$| = 1;


if($ARGV[0] eq 'create') { exit system(qq|schtasks /create /tn "$ARGV[1]\\btindex\\torrents_push" /st 00:00 /sc daily /mo 1 /tr "$^X |.Cwd::abs_path(__FILE__).qq|"|); }
if($ARGV[0] eq 'delete') { exit system(qq|schtasks /delete /tn "$ARGV[1]\\btindex\\torrents_push"|); }


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

    while(defined(my $hash = $qa->dequeue()))
    {
      my $res = btindex::webapi_add($hash);
      printf("add %d\t%s\t%s\n", $id, $hash, $res->{status});

      if($res->{status} == 200)
      {
        unlink(btindex::torrent_path($hash));
      }
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

      my $res = btindex::webapi_check(@hashs);
      if($res->{status} != 200)
      {
        printf("check\terror %d\t%s\n", $res->{status}, $res->{reason});
        return;
      }

      my $add = scalar(@{$res->{missing}});
      if($add)
      {
        printf("check\t%s .. %s\t%d to add\n", substr($_[0], 0, 18), substr($_[-1], 0, 18), $add);
        $qa->enqueue(@{$res->{missing}});
      }

      foreach my $hash (@{$res->{known}})
      {
        unlink(btindex::torrent_path($hash));
      }
    };

    my @hashs;
    while(defined(my $hash = $qc->dequeue()))
    {
      #printf("check\t%s\t%d\t%d\n", $hash, scalar(@hashs), $qc->limit());
      push(@hashs, $hash);
      if(scalar(@hashs) == $qc->limit()) { $check->(@hashs); @hashs = (); }
    }
    if(@hashs) { $check->(@hashs); }
    $qa->end();

    print("thread check end...\n");
    return;
  });


my $ts = threads->create(sub
  {
    print("thread scan start...\n");
    my $title = '';
    foreach_torrent(
      start => uc($ARGV[0]),
      sub
      {
        my ($tf, $data) = @_;

        my @tf = split('/', $tf);

        #printf("scan\t%s\t%d\n", $tf[-1], $qc->pending());
        $qc->enqueue($tf[-1]);
        if($tf[-2] ne $title)
        {
          Win32::Console::Title($tf[-1]);
          $title = $tf[-2];
        }

        return;
      });
    print("thread scan end...\n");

    $qc->end();
  });


while(grep { $_->is_running() } threads->list())
{
  ### todo: readkey/exit
  sleep(1);
}

$ts->join();
$tc->join();
foreach(@ta) { $_->join(); }

print("all done...\n");

exit 0;
