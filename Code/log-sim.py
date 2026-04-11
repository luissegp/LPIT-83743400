from datetime import datetime

import time
import os
import sys
import re
import argparse
import configparser

version = "1.03"

# NOTES:
# - \033[F = Moves the cursor up one line
# - \033[E = Moves the cursor down one line and places it at the beginning of that line
# - \033[K = Clear line to the end of the line

def _make_gen(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024*1024)

def rawgencount(filename):
    f = open(filename, 'rb')
    f_gen = _make_gen(f.raw.read)
    return sum(buf.count(b'\n') for buf in f_gen)

def find_dt_pos(text):
    pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}'

    matches = re.finditer(pattern, text)
    pos = [(match.start(), match.end()) for match in matches]

    return pos

def main():
    
    out_log_file = "cu-lan-ho.log"
    inp_log_file = os.path.join("../Data", "cu-lan-ho.log")

    print("-------------------------")
    print("srsRAN log Simulator " + version)
    print("-------------------------")
    print("")

    print("Environment:")
    if sys.platform == 'linux':
        print("- OS: Ubuntu")
    elif sys.platform == 'win32':
        print("- OS: Windows")
    else:
        print("- OS: unknown")
        sys.exit()

    print("")

    # Command line arguments
    parser = argparse.ArgumentParser(description='log-parser-sim')
    parser.add_argument('-s', '--speed', help='Simulation speed (Default=1)', required=False, type=int, default=1)

    args = vars(parser.parse_args())

    sim_speed = args['speed']

    print("Options:")
    print("- Simulation speed:", sim_speed)
    print("")

    user = os.getlogin()

    key_int = False

    comm_dt_log_ref = None

    ic = 0  # Iterations counter

    # Open the output file for writing
    with open(out_log_file, 'w', encoding='utf-8') as out_file:

        try:

            len_input = rawgencount(inp_log_file)
            print("- Input log file: " + inp_log_file + " (" + str(len_input) + " lines)")
            print("- Output log file: " + out_log_file)
            print("")

            while True:

                out_file.write("log-parser Simulator " + version + '\n')
                out_file.write("Current datetime: " + datetime.now().isoformat() + '\n')
                out_file.write("Input log file: " + inp_log_file + '\n')
                out_file.write("Output log file: " + out_log_file + '\n')
                out_file.write("\n")

                out_file.flush()

                dt_ref_log = None
                dt_ref_now = None

                ic+=1  # Iterations counter

                print("Iteration:", ic, "                           ")
                # Open the input file for reading
                with open(inp_log_file, 'r', encoding='utf-8') as inp_file:

                    t_start = time.time()

                    lc = 0  # Lines counter

                    for line in inp_file:

                        pos_dt = find_dt_pos(line)

                        if pos_dt:

                            # Extract date-time (ISO 8601) from log line
                            dt_iso = line[pos_dt[0][0]:pos_dt[0][1]]
                            dt_log = datetime.fromisoformat(dt_iso)

                            # Initialize date-time references (only once)
                            if not dt_ref_log:
                                
                                dt_ref_log = dt_log
                                dt_ref_now = datetime.now()

                                print("Reference log date-time:", dt_ref_log)
                                print("Reference now date-time:", dt_ref_now)

                            ellapsed_log = dt_log - dt_ref_log

                            # Synchronize lines generation with log time-stamps
                            while True:
                                                                
                                dt_now = datetime.now()
                                
                                ellapsed_now = dt_now - dt_ref_now

                                if time.time() - t_start > 1:

                                    dt_sim = dt_ref_log + ellapsed_now

                                    sys.stdout.write(f'Line: {lc}          \n')
                                    sys.stdout.write(f'- log ellapsed = {ellapsed_log}          \n')
                                    sys.stdout.write(f'- now ellapsed = {ellapsed_now}          \n')
                                    sys.stdout.write(f'- now dt = {dt_now}          \n')
                                    sys.stdout.write(f'- sim dt = {dt_sim}          \n')
                                    sys.stdout.write(f'- log dt = {dt_log}          ')

                                    sys.stdout.write("\033[F\033[F\033[F\033[F\033[F")
                                    sys.stdout.flush()

                                    t_start = time.time()
                                    
                                if ellapsed_now >= ellapsed_log:
                                    break
                                else:
                                    time.sleep(0.01)
                                    

                        lc+=1  # Line counter

                        out_file.write(line)
                        out_file.flush()  # Ensure the line is written immediately

                ellaped_log = dt_log - dt_ref_log
                ellapsed_now = dt_now - dt_ref_now

                print("Final log date-time:", dt_log, "       ")
                print("Final now date-time:", dt_now, "       ")
                print("Ellapsed log date-time:", ellaped_log, "       ")
                print("Ellapsed now date-time:", ellapsed_now, "       ")
                print("                                            ")

        except KeyboardInterrupt:
            sys.stdout.write("\033[E\033[E\033[E\033[E\033[E\033[E")
            print("")
            print("")
            key_int = True
        finally:
            out_file.close()
            if key_int:
                sys.stdout.write('\x1b[2K\n')
                print("... script gracefully stopped")
            else:
                print("... script finished")

if __name__ == "__main__":
    main()