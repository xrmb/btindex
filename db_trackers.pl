#!perl


use LWP;
use Compress::Zlib;
use Term::ReadKey;
use IO::Uncompress::Gunzip;
use Win32::SearchPath;
use Whatsup;

use lib (__FILE__.'/..');
use btindex;

use strict;


if($ARGV[0] eq 'create') { exit system(qq|schtasks /create /tn "$ARGV[1]\\btindex\\db_trackers" /st 00:00 /sc minute /mo 60 /tr "$^X |.Cwd::abs_path(__FILE__).qq|"|); }
if($ARGV[0] eq 'delete') { exit system(qq|schtasks /delete /tn "$ARGV[1]\\btindex\\db_trackers"|); }

my $wget = SearchPath('wgetw') || SearchPath('wget') || die 'where is wget?';

my @ts = localtime();
my $config = btindex::config();

my $db = new btindex::tdb(file => __FILE__.'/../dbs/trackers', save => 10_000_000);
my $dbc = new btindex::tdb(file => __FILE__.'/../dbs/trackers_c', save => 10_000_000);
my $dbi = new btindex::tdb(file => __FILE__.'/../dbs/trackers_i', save => 10_000_000);
my $dbn = new btindex::tdb(file => __FILE__.'/../dbs/trackers_n', save => 10_000_000);
my $dbnd = new btindex::tdb(file => sprintf(__FILE__.'/../dbs/trackers_n_%04d%02d%02d', $ts[5]+1900, $ts[4]+1, $ts[3]), save => 10_000_000);
my $dbcd = new btindex::tdb(file => sprintf(__FILE__.'/../dbs/trackers_c_%04d%02d%02d', $ts[5]+1900, $ts[4]+1, $ts[3]), save => 10_000_000);
my $dbd = new btindex::tdb(file => sprintf(__FILE__.'/../dbs/trackers_%04d%02d%02d', $ts[5]+1900, $ts[4]+1, $ts[3]), save => 10_000_000);

$dbc->clear();
$dbi->clear();
$dbn->clear();

open(my $log, '>>', __FILE__.'/../data/db_trackers.log');

my %trackers = (
    'http://zer0day.to/fullscrape.gz' => 'zd',
    'http://coppersurfer.tk/full_scrape_not_a_tracker.tar.gz' => 'cs',
    'http://scrape.leechers-paradise.org/static_scrape' => 'lp',
    #'http://internetwarriors.net/full.tar.gz' => 'iw',
    'http://tracker.sktorrent.net/full_scrape_not_a_tracker.tar.gz' => 'sk'
  );

TRACKER: foreach my $url (keys(%trackers))
{
  my $short = (split(/\./, (split(/\//, $url))[2]))[-2];

  my $cmd = "$wget $url -O - -T 10 -q";
  if($ARGV[0])
  {
    $cmd = "cat $ARGV[0]";
  }

  warn $cmd;
  my $fh;
  if(!open($fh, '-|', $cmd))
  {
    if($log) { printf($log "%s\t%s\terror\n", ''.localtime(), $url); }
    next;
  }

  my $gz;
  if($url =~ /\.gz$/)
  {
    $gz = IO::Uncompress::Gunzip->new($fh);
  }

  my $data;
  if($gz)
  {
    if($gz->read($data, 0x10000) != 0x10000) { next; }
  }
  else
  {
    if(read($fh, $data, 0x10000) != 0x10000) { next; }
  }

  my $dbntd = new btindex::tdb(file => sprintf(__FILE__.'/../dbs/trackers_n_%04d%02d%02d_%s', $ts[5]+1900, $ts[4]+1, $ts[3], $trackers{$url}), save => 10_000_000);

  my $c = 0;
  my $ac = 0;
  if($data =~ /:\d+:\d+\n/)
  {
    print("parsing mode: 1\n");

    my $i = -1;
    my $l = $i;
    while(($i = index($data, "\n", $i+1)) != -1)
    {
      #if((ReadKey(-1) || '') eq 'x') { last TRACKER; }

      $c++;

      my $data0 = substr($data, $l+1, $i-$l);

      $data0 =~ s/%(..)/chr(hex($1))/ge;

      $data0 =~ m/:(\d+):(\d+)$/;
      my $cc = 1 * $1;
      my $ic = 1 * $2;

      my $tid = uc(unpack('H*', substr($data0, 0, 20)));

      #printf("%s\t%d\t%d\t%d\t%d\n", $tid, $cc, $ic, $i, $l);

      my $added;
      $dbd->sid($tid, add => 1);
      my $id = $db->sid($tid, add => \$added);
      if($added)
      {
        printf("%s\t%08x\t%d\t%d\t%.2f%%\n", $tid, $id, $ac, $c, $ac/$c*100);
        $ac++;

        $dbn->sid($tid, add => 1);
        $dbnd->sid($tid, add => 1);
        $dbntd->sid($tid, add => 1);
      }

      if($cc) { $dbc->sid($tid, add => 1); $dbcd->sid($tid, add => 1); }
      if($ic) { $dbi->sid($tid, add => 1); }

      if($fh && $i > length($data)-1000)
      {
        my $ndata;
        my $read;

        if($gz)
        {
          $read = $gz->read($ndata, 0x10000);
        }
        else
        {
          $read = read($fh, $ndata, 0x10000);
        }

        if($read != 0x10000)
        {
          if($gz) { $gz->close(); $gz = undef; }
          close($fh);
          $fh = undef;
        }
        $data = substr($data, $i).$ndata;
        $i = 0;
      }

      $l = $i;
    }
  }
  else
  {
    print("parsing mode: 2\n");

    my $i = -1;
    while(($i = index($data, 'd8:completei', $i+1)) != -1)
    {
      #if((ReadKey(-1) || '') eq 'x') { last TRACKER; }

      $c++;

      # 123456789_123456789_d8:completei0e10:downloadedi0e10:incompletei1ee20:
      my (undef, $cc, $dc, $ic) = split(/:/, substr($data, $i, 100), 5);
      $cc =~ m/i(\d+)e/; $cc = 1 * $1;
      $dc =~ m/i(\d+)e/; $dc = 1 * $1;
      $ic =~ m/i(\d+)e/; $ic = 1 * $1;

      my $tid = uc(unpack('H*', substr($data, $i-20, 20)));

      #printf("%s\t%d\t%d\n", $tid, $cc, $ic);

      my $added;
      $dbd->sid($tid, add => 1);
      my $id = $db->sid($tid, add => \$added);
      if($added)
      {
        printf("%s\t%08x\t%d\t%d\t%.2f%%\n", $tid, $id, $ac, $c, $ac/$c*100);
        $ac++;

        $dbn->sid($tid, add => 1);
        $dbnd->sid($tid, add => 1);
        $dbntd->sid($tid, add => 1);
      }

      if($cc) { $dbc->sid($tid, add => 1); $dbcd->sid($tid, add => 1); }
      if($ic) { $dbi->sid($tid, add => 1); }

      if($fh && $i > length($data)-1000)
      {
        my $ndata;
        my $read;

        if($gz)
        {
          $read = $gz->read($ndata, 0x10000);
        }
        else
        {
          $read = read($fh, $ndata, 0x10000);
        }

        if($read != 0x10000)
        {
          if($gz) { $gz->close(); $gz = undef; }
          close($fh);
          $fh = undef;
        }
        $data = substr($data, $i).$ndata;
        $i = 0;
      }
    }
  }

  Whatsup->record(app => 'db_trackers', $short => $c);
  if($log) { printf($log "%s\t%s\t%d\t%d\n", ''.localtime(), $url, $c, $ac); }

  last if($ARGV[0]);
}


close($log);

exit 0;