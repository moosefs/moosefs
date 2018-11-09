static inline double sizestrtod(const char *restrict str, const char **restrict endptr) {
	double val,frac;
	int f;
	val = 0.0;
	f = 0;
	while (*str>='0' && *str<='9') {
		val *= 10.0;
		val += (*str-'0');
		str++;
		f = 1;
	}
	if (*str=='.' && str[1]>='0' && str[1]<='9') {
		f = 1;
		str++;
		frac = 1.0;
		while (*str>='0' && *str<='9') {
			frac /= 10.0;
			val += (*str-'0') * frac;
			str++;
		}
	}
	if (f) {
		switch (*str) {
			case 'k':
				str++;
				val *= 1e3;
				break;
			case 'K':
				if (str[1]=='i') {
					str+=2;
					val *= 1024.0;
				}
				break;
			case 'M':
				str++;
				if (*str=='i') {
					str++;
					val *= 1048576.0;
				} else {
					val *= 1e6;
				}
				break;
			case 'G':
				str++;
				if (*str=='i') {
					str++;
					val *= 1073741824.0;
				} else {
					val *= 1e9;
				}
				break;
			case 'T':
				str++;
				if (*str=='i') {
					str++;
					val *= 1099511627776.0;
				} else {
					val *= 1e12;
				}
				break;
			case 'P':
				str++;
				if (*str=='i') {
					str++;
					val *= 1125899906842624.0;
				} else {
					val *= 1e15;
				}
				break;
			case 'E':
				str++;
				if (*str=='i') {
					str++;
					val *= 1152921504606846976.0;
				} else {
					val *= 1e18;
				}
				break;
		}
	}
	if (endptr!=NULL) {
		*endptr = str;
	}
	return val;
}
