#!perl


use LWP;
use Compress::Zlib;

use btindex;

use strict;


my $config = btindex::config();

my $dbc = new btindex::tdb(file => 'dbs/trackers_c', save => 1_000_000);
my $dbi = new btindex::tdb(file => 'dbs/trackers_i', save => 1_000_000);
my $dbn = new btindex::tdb(file => 'dbs/trackers_n', save => 1_000_000);
my $db = new btindex::tdb(file => 'dbs/trackers', save => 1_000_000);

$dbc->clear();
$dbi->clear();
$dbn->clear();

foreach my $url ( 'http://zer0day.to/fullscrape.gz',
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
    my $a = 0;
    while(($i = index($data, "\n", $i+1)) != -1)
    {
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
        printf("%s\t%08x\t%d\t%d\n", $tid, $id, $a, $c);
        $a++;

        $dbn->sid($tid, add => \$added);
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
    while(($i = index($data, 'd8:completei', $i+1)) != -1)
    {
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
        printf("%s\t%08x\t%d\t%d\n", $tid, $id, $a, $c);
        $a++;

        $dbn->sid($tid, add => \$added);
      }

      if($cc) { $dbc->sid($tid, add => \$added); }
      if($ic) { $dbi->sid($tid, add => \$added); }
    }
  }

  last if($ARGV[0] eq '.');
}
