module github.com/bqqsrc/sqler

go 1.18

require (
	github.com/bqqsrc/errer v0.1.0
	github.com/bqqsrc/loger v0.0.0
	github.com/bqqsrc/dber v0.0.0
	github.com/bqqsrc/imaper v0.0.0
)

replace (
	github.com/bqqsrc/errer v0.1.0 => ../errer
	github.com/bqqsrc/loger v0.0.0 => ../loger
	github.com/bqqsrc/dber v0.0.0 => ../dber
	github.com/bqqsrc/imaper v0.0.0 => ../imaper
)