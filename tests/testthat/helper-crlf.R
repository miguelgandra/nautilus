# Write CRLF fixture lines EXACTLY, on every platform.
#
# Little Leonardo exports are CRLF, so their fixtures must be too. But `writeLines(x, <path>, sep = "\r\n")`
# opens the file in TEXT mode, and on Windows that translates every "\n" written into "\r\n" - so the
# separator lands as "\r\r\n" and the fixture silently stops being the format it claims to be.
#
# The failure that follows is worth remembering, because it is invisible until it is not: `readLines()` also
# reads in text mode, so on Windows it sees the CRLFs collapse back and reports the RIGHT line numbers -
# `.llSkip()` returns 8, correctly. But `data.table::fread()` memory-maps the file in BINARY and sees the raw
# "\r\r\n" as two line breaks, so ITS line 8 is only line 4 of the file. The reader hands fread a skip
# computed from readLines, the two disagree, and fread starts parsing mid-header: four junk rows
# ("START DATE ...", split on whitespace) ahead of the real data. Nothing errors; the frame is just wrong.
#
# A binary connection performs no translation on any platform, so the bytes are what they say they are.
.write_crlf <- function(lines, path) {
  con <- file(path, open = "wb")
  on.exit(close(con), add = TRUE)
  writeLines(lines, con, sep = "\r\n")
  invisible(path)
}
