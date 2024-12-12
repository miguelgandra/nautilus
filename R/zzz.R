
# this will run automatically when package is attached
.onAttach <- function(libname, pkgname){

  packageStartupMessage(paste0(
    "===================================================\n",
    " nautilus v", utils::packageVersion("nautilus"), "\n",
    "---------------------------------------------------\n",
    " - process tag and video data\n",
    " - analyse movement and behaviour\n",
    "---------------------------------------------------\n",
    " for a smoother journey: help(package='nautilus')\n",
    " set sail: https://github.com/miguelgandra/nautilus\n",
    "===================================================\n"))
}

