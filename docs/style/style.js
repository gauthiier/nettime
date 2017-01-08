
var ranks = document.getElementsByClassName("thread_rank_year");

for(var i = 0; i < ranks.length; i++) {
	let r = ranks[i];
	r.state = false;
	r.addEventListener('click', function(who) {
		if(r.state) {
			r.children[1].style.display = 'none';	
		} else {
			r.children[1].style.display = 'block';
		}
		r.state = !r.state;			
	}, false);
}



