#!perl

use LWP;
use Term::ReadKey;
use Win32::Console;

use Whatsup;

use btindex;

use strict;


$| = 1;
my $config = btindex::config();

my $db = 'torrents_404';

my $db404 = new btindex::tdb(file => 'dbs/'.$db);
#my $db404 = new btindex::tdb(file => 'dbs/crawl');
#my $db404 = new btindex::tdb(file => 'dbs/tixati_a');
#my $db404 = new btindex::tdb(file => 'dbs/tixati_g');
#my $db404 = new btindex::tdb(file => 'dbs/bitsnoop_all');
my $dbgot = new btindex::tdb(file => 'dbs/torrents_got');
#my $dbtix = new btindex::tdb(file => 'dbs/torrents_tix', save => 100);

my $con = new Win32::Console();

my $tatat = 300;
my $tadd = 0;
my $tdone = 0;
my ($session) = map { (split(/\s+/, substr($_, 1)))[2] } grep { /^>/ } split(/[\n\r]+/, `query session`);
MAIN: for(;;)
{
  #if(-f 'r:/zipwait') { sleep(1); next; }

  my $tl = `tasklist /FI "SESSION eq $session"`;
  if($tl !~ /tixati.exe/i)
  {
    print("starting tixati ($session)...\n");
    system("start \"wtf ms\" /min \"c:\\Program Files\\tixati\\tixati.exe\"");
    sleep(10);
  }

  my $t = tixati_transfers();
  if(!$t) { sleep(5); next; }

  my $add = $tatat - scalar(@$t);

  ### delete "done" ###
  foreach my $i (grep { $_->{mode} eq 'offline'} @$t)
  {
    my $r = tixati_transfer_delete($i->{id});
    #printf("done\t%s: %d\n", $i->{id}, $r);
    $add++;
  }

  ### delete "first" ###
  my $top = int(0.05*$tatat);
  if(@$t > $tatat - $top)
  {
    foreach my $i (@$t)
    {
      sleep(1);
      my ($code, $content) = tixati_transfer_delete($i->{id});
      #printf("first\t%s: %d\n", $i->{id}, $r);
      $add++;
      $tadd++;
      $top--;
      last if($top == 0);
    }
  }

  if($add <= 0) { sleep(10); next; }

  #while(defined(my $tid = $db404->it_id()))
  while(defined(my $tid = $db404->random_id()))
  {
    if(-f sprintf("%s/%s/%s/%s", $config->{torrents}, substr($tid, 0, 2), substr($tid, 2, 2), $tid)) { next; }
    if(defined($dbgot->sid($tid))) { next; }
    #if($tid !~ /^6/) { next; }
    #if(defined($dbtix->sid($tid))) { next; }
    if($add == 0) { last; }
    $add--;

    sleep(1);
    my ($code, $content) = tixati_transfer_add($tid);


    if($code == 200)
    {
      #defined($dbtix->sid($tid, add => 1)) || last;
    }
    else
    {
      printf("add\t%s: %d\n%s\n", $tid, $code, $content);
    }
  }

  #if(!defined($tid))
  #{
  #  $db404 = new btindex::tdb(file => 'dbs/torrents_404');
  #}

  my $done = 0;
  my $dh;
  if(!-d $config->{tixati_torrents}) { mkdir($config->{tixati_torrents}); }
  opendir($dh, $config->{tixati_torrents}) || die;
  foreach my $t (grep { /\.(tor|torrent)$/i } readdir($dh))
  {
    my $s = $config->{tixati_torrents}.'/'.$t;
    my $tc = read_file($s) || next;
    my $ih = torrent_infohash($tc) || next;
    $ih = uc($ih);
    my $d = sprintf("%s/%s/%s/%s", $config->{torrents}, substr($ih, 0, 2), substr($ih, 2, 2), $ih);
    printf("%s <- %s\n", $d, $s);
    write_file($d, $tc);
    unlink($s);
    $tdone++;
    $done++;
  }
  closedir($dh);

  if($done) { Whatsup->record(app => 'tixati_inout', $db => $done); }


  for(1..15)
  {
    $con->Title(sprintf("%d of %d%s", $tdone, $tadd, '.' x $_));
    if((ReadKey(-1) || '') eq 'x') { print("x-key\n"); last MAIN; }
    sleep(1);
  }
}
