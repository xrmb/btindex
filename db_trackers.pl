#!perl


use LWP;
use Compress::Zlib;

use btindex;

use strict;


my $config = btindex::config();

my $db = new btindex::tdb(file => 'dbs/trackers', save => 1_000_000);


foreach my $url ( 'http://zer0day.to/fullscrape.gz',
                  'http://coppersurfer.tk/full_scrape_not_a_tracker.tar.gz',
                  'http://scrape.leechers-paradise.org/static_scrape',
                  'http://internetwarriors.net/full.tar.gz',
                  'http://tracker.sktorrent.net/full_scrape_not_a_tracker.tar.gz'
                )
{
  next if($ARGV[0] && $url !~ $ARGV[0]);

  my $out = $config->{temp}.'data';
  unlink($out); ### possible leftovers
  if($url =~ /\.gz$/) { $out .= '.gz'; }
  unlink($out);
  system("wget $url -O $out -T 10");

  next unless(-f $out);

  if($url =~ /\.gz$/)
  {
    system("gzip -d $out");
    $out =~ s/.gz$//;
  }

  open(my $fh, '<', $out) || next;
  read($fh, my $data, 400_000_000) || next;
  close($fh);
  unlink($out);

  if(substr($data, 0, 1000) =~ /:\d+:\d+\n/)
  {
    my $i = -1;
    my $l = $i;
    my $c = 0;
    my $a = 0;
    while(($i = index($data, "\n", $i+1)) != -1)
    {
      $c++;

      my $tid = substr($data, $l+1, $i-$l);
      $tid =~ s/%(..)/chr(hex($1))/ge;
      $tid = uc(unpack('H*', substr($tid, 0, 20)));

      my $added;
      my $id = $db->sid($tid, add => \$added);
      if($added)
      {
        printf("%s\t%08x\t%d\t%d\n", $tid, $id, $a, $c);
        $a++;
      }

      $l = $i;
    }
  }
  else
  {
    my $i = -1;
    my $c = 0;
    while(($i = index($data, 'i0ee20:', $i+1)) != -1)
    {
      $c++;

      my $tid = uc(unpack('H*', substr($data, $i+7, 20)));
      my $added;
      my $id = $db->sid($tid, add => \$added);
      if($added)
      {
        printf("%s\t%08x\t%d\t%d\n", $tid, $id, $a, $c);
        $a++;
      }
    }
  }
}
