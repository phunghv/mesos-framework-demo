
sub get_abc {
	my ($a) = @_;
	my $b;
	eval {
		a=b;
	};
	if($@) {
		write_log("WARNING","A error: ". $@);
		return;
	}
	my $host;

	if($@) {
		write_log("WARNING","Connection error: ". $@);
		return;
	}
	return $host->get_property("c");
}