#!perl

use LWP;
use Term::ReadKey;
use Win32::Console;
use Getopt::Long;

use Whatsup;

use btindex;

use utf8;
use Encode;

use strict;


$| = 1;
my $config = btindex::config();

my $db = $config->{tixati_inout_db} || 'rss';
my $start;
my $random = $config->{tixati_inout_random};
my $inst = $config->{tixati_inout_inst} || 1;
GetOptions('db=s' => \$db, 'start=s' => \$start, random => \$random, 'inst=i' => \$inst) || die;


my $remote;
my $local;
#my $dbdo;
#my $dbgot;
#my $dbfs;
if($db =~ s/^remote://)
{
  $remote = [];
}
else
{
  $local = [];
  die unless(-f btindex::config('dbs').'/'.$db);
  #$dbdo = new btindex::tdb(file => $db);
  #$dbgot = new btindex::tdb(file => 'torrents_got');
  #$dbfs = new btindex::tdb(file => 'torrents_fs');

  #if($start) { $dbdo->set_it_id($start); }
}

my $con = new Win32::Console();

my $tatat = 300;
my $tadd = 0;
my $tdone = 0;
my ($session) = map { (split(/\s+/, substr($_, 1)))[2] } grep { /^>/ } split(/[\n\r]+/, `query session`);
MAIN: for(;;)
{
  if((ReadKey(-1) || '') eq 'x') { print("x-key\n"); last MAIN; }

  my $tl = `tasklist /FI "SESSION eq $session"`;
  foreach my $ic (1..$inst)
  {
    if($tl !~ /tixati_$ic.exe/i)
    {
      print("starting tixati $ic (session $session)...\n");
      system("start \"wtf ms\" /min \"c:\\Program Files\\tixati\\$ic\\tixati_$ic.exe\"");
      sleep(10);
    }

    my $add;
    my ($code, $t) = tixati_transfers($ic);
    if($code != 200) { printf("%d: transfers\t%d\n", $ic, $code); sleep(5); next; }
    if(!$t) { sleep(5); next; }

    $add = $tatat - scalar(@$t);

    ### delete "done" ###
    foreach my $i (grep { $_->{mode} eq 'offline'} @$t)
    {
      my $r = tixati_transfer_delete($ic, $i->{id});
      #printf("done\t%s: %d\n", $i->{id}, $r);
      $add++;
    }

    ### delete "first" ###
    my $top = int(0.05*$tatat);
    if(@$t > $tatat - $top)
    {
      foreach my $i (@$t)
      {
        my $r = tixati_transfer_delete($ic, $i->{id});
        sleep(1);
        #my ($code, $content) = tixati_transfer_delete($ic, $i->{id});
        #printf("first\t%s: %d\n", $i->{id}, $r);
        $add++;
        $tadd++;
        $top--;
        last if($top == 0);
      }
    }

    if($add <= 0) { sleep(10); next; }

    for(;;)
    {
      my $tid;
      if($remote)
      {
        if(!@$remote)
        {
          my $res = btindex::webapi_get(100, $db, 'torrents_got', 'torrents_fs');
          if($res->{status} != 200)
          {
            printf("webapi get error: %s\n", $res->{status});
            sleep(15);
            next;
          }

          $remote = $res->{data};
        }
        $tid = shift(@$remote) || next;
      }
      else
      {
        #$tid = $random ? $dbdo->random_id() : $dbdo->it_id();
        if(!@$local)
        {
          $local = btindex::tdb_get(1000, $db, 'torrents_got', 'torrents_fs');
        }
        $tid = shift(@$local) || next;
      }
      last unless($tid);


      #if($dbfs && defined($dbfs->sid($tid))) { next; }
      #if($dbgot && defined($dbgot->sid($tid))) { next; }
      if(-f btindex::torrent_path($tid)) { next; }

      if(!$remote)
      {
        ### webapi check ###
      }

      sleep(1);
      my ($code, $content) = tixati_transfer_add($ic, $tid);
      if($code != 200)
      {
        printf("%d: add\t%s: %d\n", $ic, $tid, $code);
      }

      $add--;
      if($add == 0) { last; }
    }
  }


  my $done = 0;
  my $dh;
  if(!-d $config->{tixati_torrents}) { mkdir($config->{tixati_torrents}); }
  opendir($dh, $config->{tixati_torrents}) || die;
  foreach my $t (readdir($dh))
  {
    next if($t =~ /^\.+$/);
    if($t !~ /\.(tor|torrent)$/i) { warn("$t???"); next; }
    
    my $s = $config->{tixati_torrents}.'/'.$t;
    my $tc = read_file($s);
    if(!$tc)
    {
      my $rnd = int(10000*rand());
      my $cmd = qq|ren "$config->{tixati_torrents}\\*.torrent" "zz$rnd.tor" 2>NUL|;
      system($cmd);
      warn('read_file error');
      next;
    }

    my $ih = btindex::torrent_infohash3($tc);
    if(!$ih)
    {
      print("cant hash $s\n");
      next;
    }
    $ih = uc($ih);
    unlink($s) || warn("unlink $s -> $!");
    my $write = 1;

    if($config->{'type'} ne 'master' && $config->{'webapi'})
    {
      my $res = btindex::webapi_add($ih, $tc);
      if($res->{success})
      {
        $write = 0;
      }
      else
      {
        printf("webapi add error: %s\n", $res->{status});
      }
    }

    printf("%s %s %s\n", scalar(localtime), $ih, $s);
    if($write)
    {
      my $d = btindex::torrent_path($ih);
      write_file($d, $tc);
    }

    $tdone++;
    $done++;
  }
  closedir($dh);

  if($done) { Whatsup->record(app => 'tixati_inout', $db => $done); }


  for(1..15)
  {
    $con->Title(sprintf("%d %.1f %s", $tdone, 3600*$tdone/(time()-$^T), $db, '.' x $_));
    if((ReadKey(-1) || '') eq 'x') { print("x-key\n"); last MAIN; }
    sleep(1);
  }
}
