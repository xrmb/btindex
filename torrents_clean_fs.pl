#!perl

use btindex;

use strict;


$| = 1;

my $config = btindex::config();

my $dbfs = new btindex::tdbd(file => 'dbs/torrents_fs');


my $files = 0;
my $size = 0;
while(defined(my $tid = $dbfs->it_id()))
{
  my $fn = sprintf("%s/%s/%s/%s", $config->{torrents}, substr($tid, 0, 2), substr($tid, 2, 2), $tid);
  next if(!-f $fn);

  $files++;
  $size += -s $fn;
  unlink($fn);

  printf("%s\t%d files\t%.1f mb\n",$fn, $files, $size / 1024 / 1024);
}
