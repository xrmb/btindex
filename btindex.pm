package btindex;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT = qw(foreach_log logs foreach_torrent foreach_logentry mybdecode read_file write_file run_only_once torrent_infohash
                 ipaddr load_hash save_hash sevenbin devnull torrent_path_dir
                 tixati_transfers tixati_transfer_delete tixati_transfer_add);
our @EXPORT_OK = @EXPORT;

use threads;
use threads::shared;

use Win32API::Registry qw(:ALL);
use Convert::Bencode_XS;
use Archive::Zip;
use File::Path qw(make_path);
use Digest::SHA1;
use HTTP::Tiny;
use LWP;
use JSON::PP;
#use Win32::Process::Info; ### not thread safe
use Math::Random;
use IO::Socket::SSL; IO::Socket::SSL->VERSION(1.42); ### this seems to make any initial https communication faster

use strict;


my $config :shared;
sub config
{
  my ($key, $default) = @_;
  unless($config)
  {
    my $fh;
    if(open($fh, '<', __FILE__.'/../config.dat'))
    {
      $config = shared_clone({ split(/[\t\n\r]+/, join('', grep { /^[^;#]/ } <$fh>)) });
      close($fh);
    }
    else
    {
      warn('cant open config.dat');
      $config = shared_clone({});
    }
  }

  if($key) { return $config->{$key} || $default; }
  return { %$config };
}



sub launched_by
{
  warn 'dont use me';
  return;

  my $pihandle = Win32::Process::Info->new();
  my @procinfo = $pihandle->GetProcInfo();

  my $ParentPID;
  my %ProcNames;

  foreach my $PIDInfo (@procinfo)
  {
    $ProcNames{$PIDInfo->{ProcessId}} = $PIDInfo->{Name};

    if ($PIDInfo->{ProcessId} == $$)
    {
      $ParentPID = $PIDInfo->{ParentProcessId};
    }
  }
}



sub foreach_log
{
  my $exec = pop(@_);
  my %args = @_;

  my $started = 0;

  opendir(my $dh1, 'logs') || die $!;
  foreach my $l1 (sort grep { /^dht\.log\.\d{8}\.\d{2}/ } readdir($dh1))
  {
    my $log = "logs/$l1";
    if($args{start} && $log eq $args{start}) { $started = 1; }
    if($args{start} && !$started) { next; }

    if($args{complete} && $log !~ /\.(gz|bz2|7z)$/) { next; }

    if($exec->($log)) { last; }
  }
  closedir($dh1);
}



sub foreach_logentry
{
  my $exec = pop(@_);
  my %args = @_;
  opendir(my $dh1, 'logs') || die $!;
  my @logs = sort grep { /^dht\.log\.(\d{8})\.(\d{2})/ } readdir($dh1);
  my $ts = $1.$2.'0000';
  if($args{reverse}) { @logs = reverse(@logs); }
  my $r;
  foreach my $l1 (@logs)
  {
    my $log = "logs/$l1";

    if($args{complete} && $log !~ /\.(gz|bz2|7z)$/) { next; }

    if($args{start}) { next if($args{start}->($log, $ts)); }

    my $fh;
    if($log =~ /gz$/)     { open($fh, '-|', "zcat $log") || next; }
    elsif($log =~ /7z$/)  { open($fh, '-|', "7za x -bd -so $log 2> /dev/null") || next; }
    elsif($log =~ /bz2$/) { open($fh, '-|', "bzcat $log") || next; }
    else                  { open($fh, '<', $log) || next; }

    while(my $l = <$fh>)
    {
      chomp($l);
      my @l = split(/\t/, $l);

      $r = $exec->(@l);
      last if($r);
    }

    close($fh);

    if($args{end}) { $r = $args{end}->($log, $ts); }
    last if($r);
  }
  closedir($dh1);
  return $r;
}



sub logs
{
  my $dh;
  opendir($dh, 'logs');
  my @e = sort map { "logs/$_" } grep { /^dht\.log\.\d{8}\.\d{2}/ } readdir($dh);
  closedir($dh);
  return @e;
}


sub torrent_data
{
  my ($tid, $checkzip) = @_;
  my $tf = torrent_path($tid);
  if(-f $tf) { return read_file($tf); }

  return unless $checkzip;

  my $l1 = substr($tid, 0, 2);
  my $l2 = substr($tid, 2, 2);
  my $zip = Archive::Zip->new();
  if($zip->read(__FILE__."/../torrents/$l1/$l2.zip") != 0) { warn "zip broken? $l1/$l2.zip"; return; }
  return $zip->contents($tid);
}


sub torrent_decode($)
{
  my ($dc) = @_;

  my $d;
  eval { $d = Convert::Bencode_XS::bdecode($dc); };
  if($@)
  {
    $dc =~ s/:([\w\.]+)i-?\d.\d+E\+\d+e/:${1}i1e/g;
    eval { $d = Convert::Bencode_XS::bdecode($dc); };
    if($@)
    {
      $@ =~ s/(pos \d+), .*$/$1/s;
      return wantarray ? (undef, $@) : undef;
    }
  }

  return wantarray ? ($d, 'ok') : $d;
}


sub read_file
{
  my ($f) = @_;

  my $d;
  my $fh;
  my @s = stat($f);
  if(!@s) { return; }
  open($fh, '<', $f) || die $!;
  binmode($fh);
  read($fh, $d, $s[7]);
  close($fh);

  return $d;
}



sub write_file #($$;$)
{
  my ($f, $d, $t) = @_;

  my $dir = $f;
  $dir =~ s![\/\\][^\/\\]+$!!;
  if(!-d $dir)
  {
    make_path($dir);
    if(!-d $dir) { die "cant create dir $dir"; }
  }

  my $fh;
  open($fh, '>', $f) || die "$!/$f";
  binmode($fh);
  print($fh $d);
  close($fh);

  if($t) { utime($t, $t, $f); }

  return $d;
}



sub torrent_path($)
{
  my ($hash) = @_;
  $hash = uc($hash);
  return sprintf('%s/%s/%s/%s', config('torrents'), substr($hash, 0, 2), substr($hash, 2, 2), $hash);
}



sub torrent_path_dir($)
{
  my ($hash) = @_;
  $hash = uc($hash);
  return sprintf('%s/%s/%s', config('torrents'), substr($hash, 0, 2), substr($hash, 2, 2));
}



sub foreach_torrent
{
  my $exec = pop(@_);
  my %args = @_;

  my $r = 0;
  my $tdir = config('torrents');

  opendir(my $dh1, $tdir) || die "$! ($tdir)";
  my @l1 = sort grep { /^[0-9A-F]{2}$/ } readdir($dh1);
  closedir($dh1);

  if(!exists($args{fromfs}) || $args{fromfs})
  {
    L1: foreach my $l1 (@l1)
    {
      if($args{start} && $l1 lt substr($args{start}, 0, 2)) { next; }

      opendir(my $dh2, "$tdir/$l1") || die $!;
      my @l2 = sort grep { /^[0-9A-F]{2}$/ } readdir($dh2);
      closedir($dh2);

      foreach my $l2 (@l2)
      {
        if($args{start} && "$l1$l2" lt substr($args{start}, 0, 4)) { next; }

        opendir(my $dh3, "$tdir/$l1/$l2") || die $!;
        my @l3 = sort grep { /^[0-9A-F]{40}$/ } readdir($dh3);
        closedir($dh3);

        foreach my $l3 (@l3)
        {
          if($args{start} && $l3 lt $args{start}) { next; }
          if($args{end} && $l3 gt $args{end}) { last L1; }

          my $tf = "$tdir/$l1/$l2/$l3";
          if($args{mtime} && (stat($tf))[9] < $args{mtime}) { next; }

          if($args{invalid} || !$args{invalid} && -s $tf > 3)
          {
            my $d = {};
            my $l = 0;
            if($args{data} eq 'raw')
            {
              $d = read_file($tf);
              $l = length($d);
            }
            elsif($args{data})
            {
              my $td = read_file($tf);
              $l = length($td);
              if($td !~ /^d/ || $td !~ /e$/)
              {
                warn($tf);
                rename($tf, $tf.'.broken');
                next;
              }
              eval { $d = Convert::Bencode_XS::bdecode($td); };
              if($@)
              {
                my $msg = $@;
                $msg =~ s/(pos \d+)[\0^\0]*/$1/;
                warn("$tf -> $msg");
                rename($tf, $tf.'.broken');
                next;
              }
            }

            $r = $exec->($tf, $d, $l);
            if($r) { last; }
          }
        }
        if($r) { last; }
      }
      if($r) { last; }
    }
    if($r) { return; }
  }

  if($args{fromzip})
  {
    D1: foreach my $d1 (0..255)
    {
      my $l1 = sprintf("%02X", $d1);
      if($args{start} && $l1 lt substr($args{start}, 0, 2)) { next; }

      foreach my $d2 (0..255)
      {
        my $l2 = sprintf("%02X", $d2);
        if($args{start} && $l1.$l2 lt substr($args{start}, 0, 4)) { next; }
        if($args{end} && $l1.$l2 gt substr($args{end}, 0, 4)) { last D1; }

        if($args{data})
        {
          if($args{mtime} && (stat("torrents/$l1/$l2.zip"))[9] < $args{mtime}) { next; }

          my $zip = Archive::Zip->new();
          if($zip->read("torrents/$l1/$l2.zip") != 0) { die $!; } # Archive::Zip::AZ_OK
          foreach my $m (sort { $a->fileName() cmp $b->fileName() } grep { $_->fileName() =~ m!^[0-9A-F]{40}$! } $zip->members())
          {
            if($args{start} && $m->fileName() lt $args{start}) { next; }
            if($args{mtime} && scalar($m->lastModTime()) < $args{mtime}) { next; }

            my $tf = "torrents/$l1/$l2/".$m->fileName();
            my $d = {};
            my $l = 0;

            my $td = $m->contents();
            $l = length($td);
            if($td !~ /^d/ || $td !~ /e$/)
            {
              warn($tf);
              next;
            }
            if($args{data} eq 'raw')
            {
              $d = $td;
            }
            else
            {
              eval { $d = Convert::Bencode_XS::bdecode($td); };
              if($@)
              {
                my $msg = $@;
                $msg =~ s/(pos \d+)[\0^\0]*/$1/;
                warn("$tf -> $msg");
                next;
              }
            }

            $r = $exec->($tf, $d, $l);
            if($r) { return; }
          }
        }
        else
        {
          if($args{mtime}) { die 'todo'; }

          my $fh;
          my @l;
          open($fh, '-|', "unzip -l torrents/$l1/$l2.zip") || die $!;
          while(my $l = <$fh>)
          {
            $l =~ s/^\s+|\s+$//g;
            next unless($l =~ m![0-9A-F]{40}!);
            my ($size, undef, undef, $fn) = split(/\s+/, $l);
            push(@l, "$tdir/$l1/$l2/$fn", $size);
          }
          close($fh);

          while(@l)
          {
            $r = $exec->(shift(@l), undef, shift(@l));
            if($r) { return; }
          }
        }
      }
    }
  }

  if($args{frommzip})
  {
    opendir(my $dh1, 'torrents') || die $!;
    L1: foreach my $l1 (sort grep { /^m[0-9A-F]{2}.zip$/ } readdir($dh1))
    {
      if($args{start} && substr($l1, 1, 2) lt substr($args{start}, 0, 2)) { next; }

      if($args{data})
      {
        my $zip = Archive::Zip->new();
        if($zip->read("torrents/$l1") != 0) { die $!; } # Archive::Zip::AZ_OK
        foreach my $m (sort { $a->fileName() cmp $b->fileName() } grep { !$_->isDirectory() && $_->fileName() =~ m!^[0-9A-F]{2}/[0-9A-F]{40}$! } $zip->members())
        {
          if($args{start} && substr($m->fileName(), 3) lt $args{start}) { next; }
          if($args{end} && substr($m->fileName(), 3) gt $args{end}) { next; }

          my $tf = "torrents/$l1/".$m->fileName();
          my $d = {};
          my $l = 0;

          my $td = $m->contents();
          $l = length($td);
          if($td !~ /^d/ || $td !~ /e$/)
          {
            warn($tf);
            next;
          }
          eval { $d = Convert::Bencode_XS::bdecode($td); };
          if($@)
          {
            my $msg = $@;
            $msg =~ s/(pos \d+)[\0^\0]*/$1/;
            warn("$tf -> $msg");
            next;
          }

          $r = $exec->($tf, $d, $l);
          if($r) { last; }
        }
        if($r) { last; }
      }
      else
      {
        my $fh;
        my @l;
        open($fh, '-|', "unzip -l torrents/$l1") || die $!;
        while(my $l = <$fh>)
        {
          $l =~ s/^\s+|\s+$//g;
          next unless($l =~ m![0-9A-F]{2}/[0-9A-F]{40}!);
          my ($size, undef, undef, $fn) = split(/\s+/, $l);
          push(@l, "torrents/$l1/$fn", $size);
        }
        close($fh);

        while(@l)
        {
          $r = $exec->(shift(@l), undef, shift(@l));
          if($r) { last; }
        }
        if($r) { last; }
      }
    }
    closedir($dh1);
    if($r) { last; }
  }

  if($r) { return; } ### just in case we add more after this
}





sub mybdecode {
  my $string = shift;
  my @chunks = split(//, $string);
  my $root = _dechunk(\@chunks);
  return $root;
}

sub _dechunk {
  my $chunks = shift;

  my $item = shift(@{$chunks});
  if($item eq 'd') {
    die unless(@$chunks);
    $item = shift(@{$chunks});
    my %hash;
    while($item ne 'e') {
      unshift(@{$chunks}, $item);
      my $key = _dechunk($chunks);
      if($key eq 'pieces')
      {
        my $p = [];
        #my $s = uc(join('', map { sprintf("%02X", ord($_)) } split(//, _dechunk($chunks))));
        my $s = uc(unpack('H*', _dechunk($chunks)));
        while($s) { push(@$p, substr($s, 0, 40, '')); }
        $hash{$key} = $p;
      }
      else
      {
        $hash{$key} = _dechunk($chunks);
      }
      die unless(@$chunks);
      $item = shift(@{$chunks});
    }
    return \%hash;
  }
  if($item eq 'l') {
    die unless(@$chunks);
    $item = shift(@{$chunks});
    my @list;
    while($item ne 'e') {
      unshift(@{$chunks}, $item);
      push(@list, _dechunk($chunks));
      die unless(@$chunks);
      $item = shift(@{$chunks});
    }
    return \@list;
  }
  if($item eq 'i') {
    my $num;
    die unless(@$chunks);
    $item = shift(@{$chunks});
    while($item ne 'e') {
      $num .= $item;
      die unless(@$chunks);
      $item = shift(@{$chunks});
    }
    return $num;
  }
  if($item =~ /\d/) {
    my $num;
    while($item =~ /\d/) {
      $num .= $item;
      die unless(@$chunks);
      $item = shift(@{$chunks});
    }
    my $line = '';
    for(1 .. $num) {
      die "$_/$num" unless(@$chunks);
      $line .= shift(@{$chunks});
    }
    return $line;
  }
  return $chunks;
}



sub run_only_once
{
  my $r = 0;
  my $s = $0;
  $s =~ s!.*[\\/]!!;

  if($^O eq 'MSWin32')
  {
    my $fh;
    open($fh, '-|', qq|wmic process where "CommandLine like '%$s%'" get CommandLine, ProcessID|);
    while(my $l = <$fh>)
    {
      next if($l !~ /perl\.exe/);
      my @l = split(/\s+/, $l);
      next if($l[-1] == $$);
      print("already running: $l");
      $r = 1;
    }
  }
  else
  {
    my $fh;
    open($fh, '-|', 'ps -ef');
    while(my $l = <$fh>)
    {
      next if($l !~ m!/bin/perl!);
      next if(index($l, $s) == -1);
      my @l = split(/\s+/, $l);
      next if($l[1] == $$ || $l[2] == $$);
      print("already running: $l");
      $r = 1;
    }
    close($fh);
  }

  if($r)
  {
    exit;
  }
}



sub save_hash
{
  my ($d, $f) = @_;

  my $fh;
  open($fh, '>', $f) || die $!;
  binmode($fh);
  foreach my $k (sort keys(%$d))
  {
    die "$f / $k / $d->{$k}" if($k =~ /\n/ || $d->{$k} =~ /\n/);

    print($fh $k);
    print($fh "\n");
    print($fh $d->{$k});
    print($fh "\n");
  }
  close($fh);
}


sub load_hash
{
  my ($d, $f) = @_;

  my $fh;
  open($fh, '<', $f) || die $!;
  binmode($fh);
  while(my $k = <$fh>)
  {
    chomp($k);
    my $v = <$fh>;
    chomp($v);

    if($d->{$k}) { die "$k = $v"; }
    $d->{$k} = $v;
  }
  close($fh);
}


sub torrent_infohash_dechunk
{
  my ($chunks, $at, $is, $ie) = @_;

  my $item = shift(@$chunks);
  if($item eq 'd')
  {
    $item = shift(@$chunks);
    while($item ne 'e')
    {
      unshift(@$chunks, $item);
      my $key = torrent_infohash_dechunk($chunks);
      if($key eq 'info' && $at eq '.')
      {
        $$is = scalar(@$chunks);
      }

      torrent_infohash_dechunk($chunks, $at.'.'.$key, $is, $ie);

      if($key eq 'info' && $at eq '.')
      {
        $$ie = scalar(@$chunks);
      }

      $item = shift(@$chunks);
    }
    return;
  }

  if($item eq 'l')
  {
    $item = shift(@$chunks);
    while($item ne 'e')
    {
      unshift(@$chunks, $item);
      torrent_infohash_dechunk($chunks);
      $item = shift(@$chunks);
    }
    return;
  }

  if($item eq 'i')
  {
    $item = shift(@$chunks);
    while($item ne 'e')
    {
      $item = shift(@$chunks);
    }
    return;
  }

  if($item =~ /\d/)
  {
    my $num;
    while($item =~ /\d/)
    {
      $num .= $item;
      $item = shift(@$chunks);
    }
    my $key = '';
    for(1 .. $num)
    {
      $key .= shift(@$chunks);
    }
    return $key;
  }

  die 'hmmm?';
}


sub torrent_infohash
{
  my $string = shift;
  my @chunks = split(//, $string);
  my $is = 0;
  my $ie = 0;

  torrent_infohash_dechunk(\@chunks, '.', \$is, \$ie);
  return undef unless($is && $ie);

  my $s = length($string)-$is;
  my $e = length($string)-$ie;
  if(wantarray) { return ($s, $e); }
  return uc(Digest::SHA1::sha1_hex(substr($string, $s, $e-$s)));
}



sub torrent_infohash_dechunk2_shift
{
  my ($string, $offset) = @_;
  $$offset++;
  die if($$offset > length($string));
  return substr($string, $$offset-1, 1);
}



sub torrent_infohash_dechunk2
{
  my ($string, $offset, $at, $is, $ie) = @_;

  my $item = torrent_infohash_dechunk2_shift($string, $offset);
  if($item eq 'd')
  {
    $item = torrent_infohash_dechunk2_shift($string, $offset);
    while($item ne 'e')
    {
      #unshift(@$chunks, $item);
      $$offset--;
      my $key = torrent_infohash_dechunk2($string, $offset);
      if($key eq 'info' && $at eq '.')
      {
        $$is = $$offset;#scalar(@$chunks);
      }

      torrent_infohash_dechunk2($string, $offset, $at.'.'.$key, $is, $ie);

      if($key eq 'info' && $at eq '.')
      {
        $$ie = $$offset;#scalar(@$chunks);
      }

      $item = torrent_infohash_dechunk2_shift($string, $offset);
    }
    return;
  }

  if($item eq 'l')
  {
    $item = torrent_infohash_dechunk2_shift($string, $offset);
    while($item ne 'e')
    {
      #unshift(@$chunks, $item);
      $$offset--;
      torrent_infohash_dechunk2($string, $offset);
      $item = torrent_infohash_dechunk2_shift($string, $offset);
    }
    return;
  }

  if($item eq 'i')
  {
    $item = torrent_infohash_dechunk2_shift($string, $offset);
    while($item ne 'e')
    {
      $item = torrent_infohash_dechunk2_shift($string, $offset);
    }
    return;
  }

  if($item =~ /\d/)
  {
    my $num;
    while($item =~ /\d/)
    {
      $num .= $item;
      $item = torrent_infohash_dechunk2_shift($string, $offset);
    }
    my $key = substr($string, $$offset, $num);
    $$offset += $num;
    return $key;
  }

  die 'hmmm?';
}


sub torrent_infohash2
{
  my $string = shift();
  my $offset = 0;
  my $s = 0;
  my $e = 0;

  torrent_infohash_dechunk2($string, \$offset, '.', \$s, \$e);
  return undef unless($s && $e);

  if(wantarray) { return ($s, $e); }
  return uc(Digest::SHA1::sha1_hex(substr($string, $s, $e-$s)));
}




sub torrent_infohash_dechunk3
{
  my ($string, $offset, $at, $is, $ie, $p) = @_;

  if($p) { $p->($$offset, length($string), $at); }

  my $item = substr($string, $$offset++, 1);
  if($item eq 'd')
  {
    $item = substr($string, $$offset++, 1);
    while($item ne 'e')
    {
      #unshift(@$chunks, $item);
      $$offset--;
      my $key = torrent_infohash_dechunk3($string, $offset, undef, undef, undef, $p);
      if($key eq 'info' && $at eq '.')
      {
        $$is = $$offset;#scalar(@$chunks);
      }

      torrent_infohash_dechunk3($string, $offset, $at.'.'.$key, $is, $ie, $p);

      if($key eq 'info' && $at eq '.')
      {
        $$ie = $$offset;#scalar(@$chunks);
      }

      $item = substr($string, $$offset++, 1);
    }
    return;
  }

  if($item eq 'l')
  {
    $item = substr($string, $$offset++, 1);
    while($item ne 'e')
    {
      #unshift(@$chunks, $item);
      $$offset--;
      torrent_infohash_dechunk3($string, $offset, undef, undef, undef, $p);
      $item = substr($string, $$offset++, 1);
    }
    return;
  }

  if($item eq 'i')
  {
    $item = substr($string, $$offset++, 1);
    while($item ne 'e')
    {
      $item = substr($string, $$offset++, 1);
    }
    return;
  }

  if($item =~ /\d/)
  {
    my $num;
    while($item =~ /\d/)
    {
      $num .= $item;
      $item = substr($string, $$offset++, 1);
    }
    my $key = substr($string, $$offset, $num);
    $$offset += $num;
    return $key;
  }

  die 'hmmm?';
}


sub torrent_infohash3
{
  my ($string, $p) = @_;

  my $offset = 0;
  my $s = 0;
  my $e = 0;

  torrent_infohash_dechunk3($string, \$offset, '.', \$s, \$e, $p);
  return undef unless($s && $e);

  if(wantarray) { return ($s, $e); }
  return uc(Digest::SHA1::sha1_hex(substr($string, $s, $e-$s)));
}


sub localip
{
  return '192.168.1.5';
}


my $vpnip :shared;
my $vpnip_time :shared;
sub vpnip
{
  if(!$vpnip || $vpnip_time + 180 < time())
  {
    my $if = config('vpnif');
    if($if)
    {
      my $key;
      RegOpenKeyEx(HKEY_LOCAL_MACHINE, "SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters\\Interfaces\\{$if}", 0, KEY_READ, $key) || die regLastError();
      RegQueryValueEx($key, "DhcpIPAddress", [], my $type, $vpnip, []) || die;
      RegCloseKey($key);
    }
    else
    {
      my $vpn = config('vpn') || die;
      my $ipconfig = `ipconfig`;
      $ipconfig =~ s/[\n\r]//gi;
      $ipconfig =~ m/PPP adapter $vpn:.*?IPv4 Address.*?: ([\d\.]+)/;
      $vpnip = $1;
    }
    $vpnip_time = time();
  }
  return $vpnip;
}


sub ipaddr
{
  my ($ip) = @_;
  if($ip =~ /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/ && $1 >= 0 && $1 <= 255 && $2 >= 0 && $2 <= 255 && $3 >= 0 && $3 <= 255 && $4 >= 0 && $4 <= 255)
  {
    return $ip;
  }
  if($ip =~ /^\d+$/ && $ip >= 0 && $ip <= 0xFFFF_FFFF)
  {
    return sprintf('%d.%d.%d.%d', ($ip & 0xFF00_0000) >> 24, ($ip & 0x00FF_0000) >> 16, ($ip & 0x0000_FF00) >> 8, ($ip & 0x0000_00FF));
  }
  die $ip;
}


sub badhash
{
  my ($hash) = @_;

  return 1 if($hash =~ /^0000000/ && $hash =~ /0000$/);
  return 1 if($hash =~ /0000000/ || $hash =~ /FFFFFFF/);
  return 1 if($hash =~ /13157FA15FEFE36B836589D2/);
}


sub sevenbin
{
  return ($^O eq 'MSWin32') ? '"c:\\Program Files\\7-Zip\\7z.exe"' : '7za';
}


sub devnull
{
  return ($^O eq 'MSWin32') ? 'NUL' : '/dev/null';
}


my %tixati_ua_o;
my $tixati_ua = sub
{
  my ($inst) = @_;

  #if(!$tixati_ua_o{$inst})
  {
    $tixati_ua_o{$inst} = LWP::UserAgent->new;
    $tixati_ua_o{$inst}->timeout(10);

    $tixati_ua_o{$inst}->credentials(sprintf('127.0.0.1:%d', 8888+$inst), 'Tixati Web Interface', 'a', 'a');
  }
  return $tixati_ua_o{$inst};
};



sub tixati_transfers
{
  my ($inst) = @_;

  my $ua = $tixati_ua->($inst);

  my $res = $ua->get(sprintf('http://127.0.0.1:%d/transfers', 8888+$inst));
  return ($res->code(), undef) if($res->code() != 200);

  my @i;
  my $c = $res->content();
  while($c =~ s!<tr class="(queued|downloading|offline)_(odd|even)">\s*<td>([^\0]*?)</td>\s*</tr>!!)
  {
    my $i = { mode => $1 };
    my @d = split(m!</td>\s*<td>!, $3);

    next unless($d[0] =~ /name="([0-9a-f]+)"/);
    $i->{id} = $1;

    next unless($d[1] =~ m!<a.*>(.*?)</a>!);
    $i->{name} = $1;

    push(@i, $i);

    #printf("%d\t%s %s\n", scalar(@i), $i->{id}, substr($i->{name}, 0, 20));
  }

  return ($res->code(), \@i);
}



sub tixati_transfer_delete
{
  my ($inst, $id) = @_;

  my $ua = $tixati_ua->($inst);

  my $res = $ua->post(sprintf('http://127.0.0.1:%d/transfers/action', 8888+$inst), [
    $id         => 1,
    deleteconf  => 1]);

  return wantarray ? ($res->code(), $res->decoded_content()) : $res->code();
}



sub tixati_transfer_add
{
  my ($inst, $tid) = @_;

  my $ua = $tixati_ua->($inst);

  my $res = $ua->post(sprintf('http://127.0.0.1:%d/transfers/action', 8888+$inst), [
    addlinktext	=> $tid,
    addlink	=> 'Add',
    noautostart	=> '1']);

  return wantarray ? ($res->code(), $res->decoded_content()) : $res->code();
}



sub webapi_check
{
  my (@hashs) = @_;

  my $webapi = config('webapi') || die 'please set webapi in config';

  my $ua = new HTTP::Tiny();

  my $res = $ua->post($webapi.'/mcheck/', {
      headers => { 'Content-Type' => 'application/json' },
      content => JSON::PP::encode_json(\@hashs)
    });

  if($res->{status} != 200)
  {
    return $res;
  }

  my $codes = JSON::PP::decode_json($res->{content});
  my @mhashs;
  my @khashs;
  while(@$codes)
  {
    my $hash = shift(@hashs);
    my $r = shift(@$codes);
    if($r == 404)
    {
      push(@mhashs, $hash);
    }
    if($r == 200)
    {
      push(@khashs, $hash);
    }
  }

  $res->{missing} = \@mhashs;
  $res->{known} = \@khashs;

  return $res;
}



sub webapi_add
{
  my ($hash, $data) = @_;

  my $webapi = config('webapi') || die 'please set webapi in config';

  my $ua = new HTTP::Tiny();

  my $tf = torrent_path($hash);
  my $time = time();
  if(!$data)
  {
    $data = read_file($tf) || return { status => 500, message => 'no such file' };
    $time = (stat($tf))[9];
  }


  my $res = $ua->post($webapi.'/add/?time='.$time, {
      headers => {
        'Content-Type' => 'application/octet-stream',
        'Content-Length' => length($data),
      },
      content => $data
    });

  return $res;
}



sub webapi_get
{
  my ($count, $db, @dbx) = @_;

  my $webapi = config('webapi') || die 'please set webapi in config';

  my $ua = new HTTP::Tiny();

  my $res = $ua->get($webapi."/get/?db=$db&count=$count&".join('&', map { "dbx=$_" } @dbx));

  if($res->{status} != 200)
  {
    return $res;
  }

  $res->{data} = JSON::PP::decode_json($res->{content});

  return $res;
}



sub tdb_get
{
  my ($count, $dbi, @dbx) = @_;

  my @r;
  if($count > 100) { $count = 100; }
  if($count < 1) { $count = 1; }

  if(ref($dbi) ne 'ARRAY') { $dbi = [$dbi]; }
  my $dbp1 = [];
  my $dbp2 = ['trackers_n'];

  my %dbfh;
  my %offsets;
  foreach my $db (@dbx, @$dbi, @$dbp1, @$dbp2)
  {
    my $dbf = config('dbs').'/'.$db;
    open(my $fh, '<', $dbf) || die "$!/$dbf";
    binmode($fh);
    sysread($fh, my $offsets, 0x40000);

    $dbfh{$db} = $fh;
    $offsets{$db} = [ unpack('N' x 0x10000, $offsets) ];
  }

  my $rounds = 100;
  while(@r < $count && $rounds)
  {
    $rounds--;

    my $l1 = int(rand(256));
    my $l2 = int(rand(256));

    my %hashs;
    foreach my $db (@dbx, @$dbi, @$dbp1, @$dbp2)
    {
      my $offset = 0x40000;

      my $i;
      for($i = 0; $i < $l1*256+$l2; $i++)
      {
        $offset += $offsets{$db}[$i] * 20;
      }

      sysseek($dbfh{$db}, $offset, 0) || die;

      my $read = $offsets{$db}[$i] * 20;

      #warn("reading $db from pos $offset");

      die unless(sysread($dbfh{$db}, my $data, $read) == $read);

      $hashs{$db} = [ unpack('H40' x $offsets{$db}[$i], $data) ];

      if(0)
      {
        my @hashs = sort(@{$hashs{$db}});
        warn("$hashs[0] .. $hashs[-1] ".scalar(@hashs));
      }
    }


    if(opendir(my $dh, torrent_path_dir(sprintf('%02X%02X', $l1, $l2))))
    {
      $hashs{'*fs'} = [ map { lc($_) } grep { /^[0-9a-f]{40}$/i } readdir($dh) ];
      closedir($dh);
    }


    my %hi = map { $_ => 1 } map { @{$hashs{$_}} } @$dbi;
    my %h1 = map { $_ => 1 } map { @{$hashs{$_}} } @$dbp1;
    my %h2 = map { $_ => 1 } map { @{$hashs{$_}} } @$dbp2;
    foreach my $db (@dbx, '*fs')
    {
      foreach my $hash (@{$hashs{$db}})
      {
        if(delete($hi{$hash})) { }
        if(delete($h1{$hash})) { }
        if(delete($h2{$hash})) { }
      }
    }

    my $p = rand(1000);
    my @h = keys(%h1);
    while(@r < $count * 0.33 && @h)
    {
      push(@r, splice(@h, $p % scalar(@h), 1));
      $p += rand(1000);
    }

    @h = keys(%h2);
    while(@r < $count * 0.66 && @h)
    {
      push(@r, splice(@h, $p % scalar(@h), 1));
      $p += rand(1000);
    }

    @h = keys(%hi);
    while(@r < $count && @h)
    {
      push(@r, splice(@h, $p % scalar(@h), 1));
      $p += rand(1000);
    }
  }

  foreach my $fh (values(%dbfh)) { close($fh) };

  return [ map { uc($_) } @r ];
}


1;



###################################################################################################
###################################################################################################
###################################################################################################
package btindex::tdbd;

use File::Spec;


sub new
{
  my $class = shift();
  my $self  = {};
  bless($self, $class);
  my %args = @_;

  $self->{dbf} = File::Spec->rel2abs($args{file}) || File::Spec->rel2abs(config('dbs').'/'.$args{file}) || die 'need file';
  $self->{buffer} = undef;
  $self->{block} = -1;
  $self->{at} = undef;

  $self->{offsets} = [ unpack('N' x 0x10000, $self->_read(0, 0x40000)) ];

  return $self;
}



sub _read
{
  my $self = shift();

  my ($offset, $length) = @_;

  open(my $fh, '<', $self->{dbf}) || die $!;
  binmode($fh);

  if(sysseek($fh, $offset, 0) != $offset) { die 'seek error'; }
  my $r = sysread($fh, my $data, $length);
  if($r != $length) { die "read: want ".($length).", got: $r, length: ".length($data); }

  close($fh);

  return $data;
}



sub it_id
{
  my $self = shift();

  if($self->{at}*20 >= length($self->{buffer}))
  {
    do
    {
      $self->{block}++;
    } while($self->{block} != 0x10000 && $self->{offsets}[$self->{block}] == 0);
    return undef if($self->{block} == 0x10000);

    $self->{at} = 0;

    my $offset = 0x40000;
    for(my $i = 0; $i < $self->{block}; $i++)
    {
      $offset += $self->{offsets}[$i]*20;
    }
    $self->{buffer} = $self->_read($offset, $self->{offsets}[$self->{block}]*20);
  }

  my $id = uc(unpack('H*', substr($self->{buffer}, $self->{at}*20, 20)));

  $self->{at}++;

  return $id;
}


sub set_it_id
{
  my $self = shift();

  my ($id) = @_;

  $self->{block} = hex(substr($id.'0000', 0, 4));
  $self->{buffer} = undef;
  $self->{at} = 0;

  return;
}


1;



###################################################################################################
###################################################################################################
###################################################################################################
package btindex::tdb;

use File::Spec;


sub new
{
  my $class = shift();
  my $self  = {};
  bless($self, $class);
  my %args = @_;

  $self->{dbf} = File::Spec->rel2abs($args{file}) || File::Spec->rel2abs(config('dbs').'/'.$args{file}) || die 'need file';
  $self->{save} = $args{save} || 0;
  $self->{db} = undef;
  $self->{dbfc} = $args{reload} ? 0 : (time() + 900_000_000);
  $self->{dbfm} = 0;
  $self->{it} = undef;

  return $self;
}


sub DESTROY
{
  my $self = shift();
  if($self->{dirty}) { $self->save(); }
}


sub clear
{
  my $self = shift();
  $self->{db} = {};
}


sub load
{
  my $self = shift();

  if($self->{dbfc} + 10 < time())
  {
    $self->{dbfc} = time();

    if((!$self->{dirty} || $self->{save} < 0) && -s $self->{dbf} && $self->{dbfm} && $self->{dbfm} != (stat($self->{dbf}))[9])
    {
      if(time() - (stat($self->{dbf}))[7] < 30) { warn('changed, waiting for db to age...'); return; }
      printf("reloading %s (%d)...\n", $self->{dbf}, -s $self->{dbf});
      $self->{db} = undef;
    }
  }

  if($self->{db}) { return 'already loaded'; }

  if(!-f $self->{dbf}) { return 'no dbf'; }
  $self->{db} = {};

  ### todo: .new is now .new$$ ###
  if(-f $self->{dbf}.'.new') { die "a new db is present ($self->{dbf})"; }

  my $offsets;
  my @offsets;
  my $fh;
  for(;;)
  {
    open($fh, '<', $self->{dbf}) || die $!;
    binmode($fh);
    my $r = sysread($fh, $offsets, 0x40000);
    if($r != 0x40000) { die "offsets: want ".(0x40000).", got: $r, length: ".length($offsets); }
    @offsets = unpack('N' x 0x10000, $offsets);

    last if(grep { $_ } @offsets);

    warn("no offsets in $self->{dbf}?");
    sleep(10);
  }

  foreach my $i0 (0..0xFF)
  {
    my $id0 = sprintf('%02X', $i0);
    foreach my $i1 (0..0xFF)
    {
      my $o = $i0*0x100+$i1;
      next unless($offsets[$o]);
      my $id1 = sprintf('%02X', $i1);
      my $d;
      my $r = sysread($fh, $d, $offsets[$o]*20);
      if($r != $offsets[$o]*20) { die "$id0/$id1 -> want: $offsets[$o], got: ".($r).", length: ".length($d)." in $self->{dbf}"; }
      #$self->{db}{$id0}{$id1} = uc(unpack('H*', $d));
      $self->{db}{$id0}{$id1} = $d;
    }
  }
  if(read($fh, $offsets, 10_000_000)) { die "there is ".length($offsets)." left but shouldnt in ".$self->{dbf}; }
  close($fh);

  $self->{dbfm} = (stat($self->{dbf}))[9];

  return 'loaded';
}



sub tbd_load
{
  my ($db, $dbf) = @_;

  my $fh;
  open($fh, '<', $dbf) || die $!;
  binmode($fh);
  my $offsets;
  my $r = sysread($fh, $offsets, 0x40000);
  if($r != 0x40000) { die "offsets: want ".(0x40000).", got: $r, length: ".length($offsets); }
  my @offsets = unpack('N' x 0x10000, $offsets);
  foreach my $i0 (0..0xFF)
  {
    my $id0 = sprintf('%02X', $i0);
    foreach my $i1 (0..0xFF)
    {
      my $o = $i0*0x100+$i1;
      next unless($offsets[$o]);
      my $id1 = sprintf('%02X', $i1);
      my $d;
      my $r = sysread($fh, $d, $offsets[$o]*20);
      if($r != $offsets[$o]*20) { die "$id0/$id1 -> want: $offsets[$o], got: ".($r).", length: ".length($d); }
      $db->{$id0.$id1} = $d;
    }
  }
  if(read($fh, $offsets, 10_000_000)) { die "there is ".length($offsets)." left but shouldnt in ".$dbf; }
  close($fh);

  return 'loaded';
}



sub save
{
  my $self = shift();

  if(!$self->{db}) { warn($self->{dbf}.' not loaded'); return 'not loaded'; }
  if(!$self->{dirty}) { return 'not dirty'; }
  if($self->{save} < 0) { return 'save not allowed'; }

  if($self->{dbfm} && $self->{dbfm} != (stat($self->{dbf}))[9])
  {
    printf("saving changed database %s?", $self->{dbf});
    <STDIN>;
  }

  my $fh;
  open($fh, '>', $self->{dbf}.".new$$") || die "fopen: $! / ".$self->{dbf}.".new$$";
  binmode($fh);
  my $tc = 0;
  my $d = '';
  foreach my $i0 (0..0xFF)
  {
    my $id0 = sprintf('%02X', $i0);
    foreach my $i1 (0..0xFF)
    {
      my $id1 = sprintf('%02X', $i1);
      #my $c = length($self->{db}{$id0}{$id1})/40;
      my $c = length($self->{db}{$id0}{$id1})/20;
      die "$id1 -> $c" if($c > 0xFFFF);
      $d .= pack('N', $c);
      $tc += $c;
    }
  }
  print($fh $d);

  foreach my $i0 (0..0xFF)
  {
    my $id0 = sprintf('%02X', $i0);
    $d = '';
    foreach my $i1 (0..0xFF)
    {
      my $id1 = sprintf('%02X', $i1);
      #print($fh pack('H*', $self->{db}{$id0}{$id1}));
      $d .= $self->{db}{$id0}{$id1};
    }
    print($fh $d);
  }
  close($fh);

  for(1..180)
  {
    last if(!-f $self->{dbf});
    #warn 'exists '.$self->{dbf};
    if(-f $self->{dbf}.'.old') { unlink($self->{dbf}.'.old') || die $!; }
    if(!rename($self->{dbf}, $self->{dbf}.'.old'))
    {
      warn $!;
      sleep(10);
    }
  }

  ### someone is locking the file ###
  for(1..180)
  {
    last if(!-f $self->{dbf}.".new$$");
    #warn 'new there '.$self->{dbf};
    if(!rename($self->{dbf}.".new$$", $self->{dbf}))
    {
      warn $!;
      sleep(10);
    }
  }
  if(-f $self->{dbf}.".new$$") { die "new still there"; }
  unlink($self->{dbf}.'.old');

  $self->{dirty} = 0;
  $self->{dbfm} = (stat($self->{dbf}))[9];

  return $tc;
}



sub sort
{
  my $self = shift();

  if(!$self->{db}) { return 'nothing loaded'; }

  foreach my $i0 (0..0xFF)
  {
    my $id0 = sprintf('%02X', $i0);
    foreach my $i1 (0..0xFF)
    {
      my $id1 = sprintf('%02X', $i1);
      $self->{db}{$id0}{$id1} = pack('H*', join('', sort unpack('H40' x (length($self->{db}{$id0}{$id1}) / 20), $self->{db}{$id0}{$id1})));
    }
  }

  $self->{dirty} = 1;

  return 'sorted';
}



sub remove
{
  my $self = shift();

  my ($tid, %args) = @_;
  my $tidp;

  if(length($tid) == 20)
  {
    $tidp = $tid;
    $tid = uc(unpack('H*', $tidp));
  }
  elsif($tid =~ /^([0-9A-F]{40})$/)
  {
    $tid = $1; ### possible \n at the end
    $tidp = pack('H*', $tid);
  }
  else
  {
    die "tid: $tid?";
  }

  $self->load();


  my $id0 = substr($tid, 0, 2);
  my $id1 = substr($tid, 2, 2);
  my $i = 0;
  #while($i = index($self->{db}{$id0}{$id1}, $tid, $i))
  while($i = index($self->{db}{$id0}{$id1}, $tidp, $i))
  {
    last if($i == -1);
    #last if($i % 40 == 0);
    last if($i % 20 == 0);
    $i++;
  }
  if($i == -1)
  {
    return undef;
  }

  substr($self->{db}{$id0}{$id1}, $i, length($tidp), '');
  if($self->sid($tid)) { die "still there after remove?"; }

  $self->{dirty}++;

  return (hex(unpack('H4', $tidp)) << 16) + $i / 20;
}



sub sid
{
  my $self = shift();

  my ($tid, %args) = @_;
  my $tidp;

  if(length($tid) == 20)
  {
    $tidp = $tid;
    $tid = uc(unpack('H*', $tidp));
  }
  elsif($tid =~ /^([0-9A-F]{40})$/)
  {
    $tid = $1; ### could have \n at the end
    $tidp = pack('H*', $tid);
  }
  else
  {
    warn "invalid id $tid";
    return undef;
  }

  $self->load();


  my $id0 = substr($tid, 0, 2);
  my $id1 = substr($tid, 2, 2);
  my $i = 0;
  #while($i = index($self->{db}{$id0}{$id1}, $tid, $i))
  while($i = index($self->{db}{$id0}{$id1}, $tidp, $i))
  {
    last if($i == -1);
    #last if($i % 40 == 0);
    last if($i % 20 == 0);
    $i++;
  }
  if($i == -1)
  {
    if(!$args{add}) { return undef; }

    $i = length($self->{db}{$id0}{$id1});
    if(ref($args{add}) eq 'SCALAR') { ${$args{add}} = 1; }
    #$self->{db}{$id0}{$id1} .= $tid;
    $self->{db}{$id0}{$id1} .= $tidp;
    $self->{dirty}++;
    if($self->{save} && $self->{dirty} >= $self->{save}) { $self->save(); }
    die if(!defined($self->sid($tid)));
  }

  #return (hex(unpack('H4', $tidp)) << 16) + $i / 40;
  return (hex(unpack('H4', $tidp)) << 16) + $i / 20;
}


sub tdb_sid
{
  my ($db, $tid) = @_;
  my $tidp;

  if(length($tid) == 20)
  {
    $tidp = $tid;
    $tid = uc(unpack('H*', $tidp));
  }
  elsif($tid =~ /^([0-9A-F]{40})$/)
  {
    $tid = $1; ### could have \n at the end
    $tidp = pack('H*', $tid);
  }
  else
  {
    warn "invalid id $tid";
    return undef;
  }


  my $id0 = substr($tid, 0, 2);
  my $id1 = substr($tid, 2, 2);
  my $i = 0;
  while($i = index($db->{$id0.$id1}, $tidp, $i))
  {
    last if($i == -1);
    last if($i % 20 == 0);
    $i++;
  }
  if($i == -1)
  {
    return undef;
  }

  return (hex(unpack('H4', $tidp)) << 16) + $i / 20;
}



sub sidp
{
  my $self = shift();
  my $sid = $self->sid(@_);
  if(!defined($sid)) { return undef; }
  return pack('N', $sid);
}



sub id
{
  my $self = shift();
  my ($sid) = @_;

  $self->load();

  my $id0 = sprintf('%02X', ($sid & 0xFF000000) >> 24);
  my $id1 = sprintf('%02X', ($sid & 0x00FF0000) >> 16);
  $sid &= 0xFFFF;
  #if($sid*40 >= length($self->{db}{$id0}{$id1})) { return undef; }
  if($sid*20 >= length($self->{db}{$id0}{$id1})) { return undef; }

  #return substr($self->{db}{$id0}{$id1}, $sid*40, 40);
  return uc(unpack('H*', substr($self->{db}{$id0}{$id1}, $sid*20, 20)));
}



sub idp
{
  my $self = shift();
  my ($sid) = @_;

  $self->load();

  my $id0 = sprintf('%02X', ($sid & 0xFF000000) >> 24);
  my $id1 = sprintf('%02X', ($sid & 0x00FF0000) >> 16);
  if($sid*20 >= length($self->{db}{$id0}{$id1})) { return undef; }

  return substr($self->{db}{$id0}{$id1}, $sid*20, 20);
}



sub set_it_id
{
  my $self = shift();
  my ($tid) = @_;

  $self->{it} = hex(substr($tid . '0000', 0, 4)) * 0x10000 -1;
}



sub it_id
{
  my $self = shift();

  $self->load();

  if(!defined($self->{it}))
  {
    $self->{it} = -1;
  }

  $self->{it}++;

  for(;;)
  {
    my $id = $self->id($self->{it});
    if($id) { return $id; }

    if(($self->{it} & 0xFFFF_0000) == 0xFFFF_0000)
    {
      $self->{it} = -1;
      return undef;
    }
    if(!$id)
    {
      $self->{it} &= 0xFFFF_0000;
      $self->{it} += 0x10000;
    }
  }
}


sub random_id
{
  my $self = shift();

  $self->load();
  return if(!exists($self->{db}));

  my $noluck = 0;
  for(;;)
  {
    if($noluck > 10000) { die localtime.": no luck? ".join(',', keys(%{$self->{db}})); }
    my $r0 = int(rand(256));
    my $id0 = sprintf('%02X', $r0);
    if(!$self->{db}{$id0}) { $noluck++; next; }

    my $r1 = int(rand(256));
    my $id1 = sprintf('%02X', $r1);
    if(!$self->{db}{$id0}{$id1}) { $noluck++; next; }

    #my $r2 = int(rand(length($self->{db}{$id0}{$id1}))/20);
    my $r2 = int(Math::Random::random_uniform_integer(1, 0, length($self->{db}{$id0}{$id1})) / 20);

    return $self->id(($r0 << 24) + ($r1 << 16) + $r2);
  }
}



sub random_idff
{
  my $self = shift();
  my ($count) = @_;

  $count ||= 1;

  my $s = int(int(((-s $self->{dbf}) - 0x40000) / 20) / $count) - $count;

  my $fh;
  open($fh, '<', $self->{dbf}) || die $!;
  binmode($fh);

  #my $offsets;
  #my $r = sysread($fh, $offsets, 0x40000);
  #if($r != 0x40000) { die "offsets: want ".(0x40000).", got: $r, length: ".length($offsets); }

  #my $r = 0;
  #for (0..10)
  #{
  #  $r += rand(32768); ### my windows RANDBITS
  #}
  #$r %= $s;

  my $r = Math::Random::random_uniform_integer(1, 0, $s);

  my $to = 0x40000 + $r * $count * 20;
  seek($fh, $to, 0) || die $!;

  read($fh, my $ih, 20*$count) == 20*$count || die $!;

  close($fh);

  return map { uc($_) } unpack('H40' x $count, $ih);
}



1;
