#!perl

use strict;

open(my $fh, '<', $ARGV[0]) || die;
binmode($fh);
read($fh, my $data, 100_000_000);
close($fh);

my $t = time();
if($data =~ s/MsgWnd2382419567/MsgWnd$t/) { print("patch 1 ok\n"); }
if($data =~ s/Local\\54634a56tb/Local\\$t/) { print("patch 2 ok\n"); }

rename($ARGV[0], $ARGV[0].'.bak') || die;

open($fh, '>', $ARGV[0]) || die;
binmode($fh);
print($fh $data);
close($fh);
