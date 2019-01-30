use strict;
use warnings;

use Test::More;
use DBI;
use DBD::MariaDB;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1 });

plan tests => 56;

ok($dbh->do("CREATE TEMPORARY TABLE t(id INT)"));
ok($dbh->do("INSERT INTO t(id) VALUES(1)"));

my $sth1 = $dbh->prepare("SELECT * FROM t");
ok($sth1->execute());
ok($sth1->{Active});
is_deeply($sth1->{NAME}, ["id"]);
is_deeply($sth1->fetchall_arrayref(), [ [ 1 ] ]);
ok(!$sth1->{Active});
is_deeply($sth1->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

my $sth2 = $dbh->prepare("SELECT * FROM t", { mariadb_server_prepare => 1 });
ok($sth2->execute());
ok($sth2->{Active});
is_deeply($sth2->{NAME}, ["id"]);
is_deeply($sth2->fetchall_arrayref(), [ [ 1 ] ]);
ok(!$sth2->{Active});
is_deeply($sth2->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

ok($dbh->do("INSERT INTO t(id) VALUES(2)"));

my $sth3 = $dbh->prepare("SELECT * FROM t");
ok($sth3->execute());
ok($sth3->{Active});
is_deeply($sth3->{NAME}, ["id"]);
is_deeply($sth3->fetchrow_arrayref(), [ 1 ]);
ok($sth3->{Active});
is_deeply($sth3->{NAME}, ["id"]);
is_deeply($sth3->fetchrow_arrayref(), [ 2 ]);
ok(!$sth3->{Active});
is_deeply($sth3->{NAME}, ["id"]);
is_deeply($sth3->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

my $sth4 = $dbh->prepare("SELECT * FROM t", { mariadb_server_prepare => 1 });
ok($sth4->execute());
ok($sth4->{Active});
is_deeply($sth4->{NAME}, ["id"]);
is_deeply($sth4->fetchrow_arrayref(), [ 1 ]);
ok($sth4->{Active});
is_deeply($sth4->{NAME}, ["id"]);
is_deeply($sth4->fetchrow_arrayref(), [ 2 ]);
ok(!$sth4->{Active});
is_deeply($sth4->{NAME}, ["id"]);
is_deeply($sth4->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

my $sth5 = $dbh->prepare("SELECT * FROM t");
ok($sth5->execute());
ok($sth5->{Active});
is_deeply($sth5->{NAME}, ["id"]);
is_deeply($sth5->fetchrow_arrayref(), [ 1 ]);
ok($sth5->{Active});
is_deeply($sth5->{NAME}, ["id"]);
ok($sth5->finish);
ok(!$sth5->{Active});
is_deeply($sth5->{NAME}, ["id"]);
is_deeply($sth5->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

my $sth6 = $dbh->prepare("SELECT * FROM t", { mariadb_server_prepare => 1 });
ok($sth6->execute());
ok($sth6->{Active});
is_deeply($sth6->{NAME}, ["id"]);
is_deeply($sth6->fetchrow_arrayref(), [ 1 ]);
ok($sth6->{Active});
is_deeply($sth6->{NAME}, ["id"]);
ok($sth6->finish);
ok(!$sth6->{Active});
is_deeply($sth6->{NAME}, ["id"]);
is_deeply($sth6->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

ok($dbh->disconnect());
