package complex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
public class ComplexBD {
    public static void put(String string) {
        System.out.print(string);
    }

    public static void puts(String string) {
        System.out.println(string);
    }

    public static void main(String[] args) throws IOException, Exception {
        put("Complex BD (c) Amitabh Saxena. Enter ? for help. Enter \"q\" to quit\n");
        new ComplexBD().startIt();
    }

    public void startIt() throws IOException, Exception {
        String string = null;
        BD.initScale(128);
        BD.initDisp(128);
        int time = 12000; 
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            try {
                put("\n> ");
                string = in.readLine().toLowerCase().trim();
                if (string.isEmpty()) {
                    continue;
                }
                if (string.equals("quit") || string.equals("exit") || string.equals("q")) {
                    System.exit(1);
                }
                if (string.equals("?") || string.equals("help") || string.equals("/?")) {
                    givhlp();
                    continue;
                }
                if (string.startsWith("scale")) {
                    String tmp1 = string.substring("scale".length()).trim();
                    if (tmp1.isEmpty()) {
                        puts("Scale is " + BD.getScale());
                        puts("This is the decimal scale of all complex numbers");
                        puts("To change, type \"scale\" followed by a number");
                    } else {
                        BD.initScale(Integer.valueOf(tmp1));
                    }
                    continue;
                }
                if (string.startsWith("time")) {
                    String tmp1 = string.substring("time".length()).trim();
                    if (tmp1.isEmpty()) {
                        puts("timeout time is " + time + " milliseconds");
                        puts("To change, type \"time\" followed by a number");
                    } else {
                        int i = Integer.valueOf(tmp1);
                        time = (i > 0) ? i : time;
                    }
                    continue;
                }
                if (string.startsWith("disp")) {
                    String tmp1;
                    if (string.startsWith("display")) {
                        tmp1 = string.substring("display".length()).trim();
                    } else {
                        tmp1 = string.substring("disp".length()).trim();
                    }

                    if (tmp1.isEmpty()) {
                        puts("Display scale is " + BD.getDisplay());
                        puts("To change, type \"display\" followed by a number");
                    } else {
                        BD.initDisp(Integer.valueOf(tmp1));
                    }
                    continue;
                }
                Compute c = new Compute();
                c.set(string);
                // check if user wants to display memory
                if (c.get().equals("m")){
                    for (int i=0;i<10;i++){
                        c.set("m"+String.valueOf(i));
                        System.out.print("m"+i+" = ");
                        c.run();
                        System.out.println();
                    }
                    continue;
                }

                long startTime = System.currentTimeMillis();
                
                c.start();
                while (true) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    if (!c.isAlive()) {
                        break;
                    }
                    if (c.isAlive() && elapsed > time) {
                        c.stop();
                        throw new ArithmeticException("timeout: operation taking too long");
                    }
                }
            } catch (Exception e) {
                System.out.println(//"Exception: " +
                        e.getMessage());
//                e.printStackTrace();
                continue;
            }
        }
    }

    public class Compute extends Thread {
        private String privateString;
        Parser p = new Parser();

        @Override
        public void run() {
                try {
                    CN cn = p.getValue(privateString);
                    System.out.print(//cn == null ? "" : "\n" +
                            cn.format(p.eform));
                } catch (Exception e) {                    
                    System.out.print(//"Exception: " +
                            e.getMessage());
                    // e.printStackTrace();
                }
        }

        public void set(String st) {
            privateString = st;
            if (privateString.charAt(0) == '!') {
                p.eform = true;
                privateString = privateString.substring(1);
            }
        }

        private String get() {
            return privateString;
        }
    }

    public static void givhlp() {
        puts("  |-----------------------------------------------------------------|");
        puts("  | ComplexBD: complex number expression evaluation with BigDecimal |");
        puts("  | Author: Amitabh Saxena (http://tinyurl.com/amitabhs)            |");
        puts("  | Version: 0.1       Status: experimental                         |");
        puts("  |-----------------------------------------------------------------|");
        puts("   Evaluates [exp], where [exp] is a complex number expression.");
        puts("   Complex number expressions are made of numbers and following symbols:");
        puts("  |----------------------------------------------------------------|");
        puts("  | +, -, *, /, ^, (, ), i, sqrt, sin, cos, tan, asin, acos, atan, |");
        puts("  | sinh, cosh, tanh, asinh, acosh, atanh, mod, arg, log, alog, pi,|");
        
        puts("  | conj, re (real part), im (imag. part), @, $, et, Z, m0...m9, = |"); 
        puts("  |----------------------------------------------------------------|");
        puts("    Example: 1+2i + 3*(4-5i) + sin(6+7.8i)\n");
        
        
        puts("                                ix");
        puts("   To input in polar form,  Re   , seperate R and x by \"" + Parser.atString + "\". Example: 1@2");
        puts("   To output in polar form, suffix \"!\" to expression. Example: !(1+2i)");
        puts("   Variables: z is the last result and m0..m9 are assigned using \"=\".");
        puts("      Examples:     m0=m1=1+2i     m1=(1+2i)     m2=(m1+1)+2i");
        puts("                    m4=(m3=(m2=(m0=1234^m1=5678)^(1/m1))-m0)");
        puts("   Order of evaluation: " + Parser.evalPriorityString);
        puts("   Other symbols: $ = et = 2.7182818..., m = view variables.");
        puts("   Other Keywords: scale, display, time, (q)uit, exit, help, ?.");
    }
}
