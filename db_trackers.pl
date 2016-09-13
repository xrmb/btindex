#!perl


use LWP;
use Compress::Zlib;
use Term::ReadKey;

use lib (__FILE__.'/..');
use btindex;

use strict;


if($ARGV[0] eq 'create') { exit system(qq|schtasks /create /tn "$ARGV[1]\\btindex\\db_trackers" /st 00:00 /sc minute /mo 180 /tr "$^X |.Cwd::abs_path(__FILE__).qq|"|); }
if($ARGV[0] eq 'delete') { exit system(qq|schtasks /delete /tn "$ARGV[1]\\btindex\\db_trackers"|); }


my @ts = localtime();
my $config = btindex::config();

my $dbc = new btindex::tdb(file => __FILE__.'/../dbs/trackers_c', save => 10_000_000);
my $dbi = new btindex::tdb(file => __FILE__.'/../dbs/trackers_i', save => 10_000_000);
my $dbn = new btindex::tdb(file => __FILE__.'/../dbs/trackers_n', save => 10_000_000);
my $dbnd = new btindex::tdb(file => sprintf(__FILE__.'/../dbs/trackers_n_%04d%02d%02d', $ts[5]+1900, $ts[4]+1, $ts[3]), save => 10_000_000);
my $db = new btindex::tdb(file => __FILE__.'/../dbs/trackers', save => 10_000_000);

$dbc->clear();
$dbi->clear();
$dbn->clear();

TRACKER: foreach my $url (
                 'http://zer0day.to/fullscrape.gz',
                  'http://coppersurfer.tk/full_scrape_not_a_tracker.tar.gz',
                  'http://scrape.leechers-paradise.org/static_scrape',
                  #'http://internetwarriors.net/full.tar.gz',
                  'http://tracker.sktorrent.net/full_scrape_not_a_tracker.tar.gz'
                )
{
  next if($ARGV[0] && $url !~ $ARGV[0]);

  my $out = $config->{temp}.'/data';

  if($ARGV[0] ne '.')
  {
    unlink($out); ### possible leftovers
    if($url =~ /\.gz$/) { $out .= '.gz'; }
    unlink($out);
    system(qq|wget $url -O $out -T 10|);

    next unless(-f $out);

    if($url =~ /\.gz$/)
    {
      system("gzip -d $out");
      $out =~ s/.gz$//;
    }
  }

  open(my $fh, '<', $out) || next;
  read($fh, my $data, 400_000_000) || next;
  close($fh);
  if($ARGV[0] ne '.') { unlink($out); }

  if(substr($data, 0, 1000) =~ /:\d+:\d+\n/)
  {
    my $i = -1;
    my $l = $i;
    my $c = 0;
    my $ac = 0;
    while(($i = index($data, "\n", $i+1)) != -1)
    {
      if((ReadKey(-1) || '') eq 'x') { last TRACKER; }

      $c++;

      my $data0 = substr($data, $l+1, $i-$l);

      $data0 =~ s/%(..)/chr(hex($1))/ge;

      $data0 =~ m/:(\d+):(\d+)$/;
      my $cc = 1 * $1;
      my $ic = 1 * $2;

      my $tid = uc(unpack('H*', substr($data0, 0, 20)));

      #printf("%s\t%d\t%d\t%d\t%d\n", $tid, $cc, $ic, $i, $l);

      my $added;
      my $id = $db->sid($tid, add => \$added);
      if($added)
      {
        printf("%s\t%08x\t%d\t%d\t%.2f%%\n", $tid, $id, $ac, $c, $ac/$c*100);
        $ac++;

        $dbn->sid($tid, add => \$added);
        $dbnd->sid($tid, add => \$added);
      }

      if($cc) { $dbc->sid($tid, add => \$added); }
      if($ic) { $dbi->sid($tid, add => \$added); }

      $l = $i;
    }
  }
  else
  {
    my $i = -1;
    my $c = 0;
    my $ac = 0;
    while(($i = index($data, 'd8:completei', $i+1)) != -1)
    {
      if((ReadKey(-1) || '') eq 'x') { last TRACKER; }

      $c++;

      # 123456789_123456789_d8:completei0e10:downloadedi0e10:incompletei1ee20:
      my (undef, $cc, $dc, $ic) = split(/:/, substr($data, $i, 100), 5);
      $cc =~ m/i(\d+)e/; $cc = 1 * $1;
      $dc =~ m/i(\d+)e/; $dc = 1 * $1;
      $ic =~ m/i(\d+)e/; $ic = 1 * $1;

      my $tid = uc(unpack('H*', substr($data, $i-20, 20)));

      #printf("%s\t%d\t%d\n", $tid, $cc, $ic);

      my $added;
      my $id = $db->sid($tid, add => \$added);
      if($added)
      {
        printf("%s\t%08x\t%d\t%d\t%.2f%%\n", $tid, $id, $ac, $c, $ac/$c*100);
        $ac++;

        $dbn->sid($tid, add => \$added);
        $dbnd->sid($tid, add => \$added);
      }

      if($cc) { $dbc->sid($tid, add => \$added); }
      if($ic) { $dbi->sid($tid, add => \$added); }
    }
  }

  last if($ARGV[0] eq '.');
}
