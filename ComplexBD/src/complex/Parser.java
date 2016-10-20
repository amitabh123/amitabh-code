package complex;

public class Parser {
	// to write in scala
	
	// this is very verbose
    // Priority for evaluation
    final static int none = 0;
    final static int add = none + 1;
    final static int subtract = add + 1;
    final static int multiply = subtract + 1;
    final static int divide = multiply + 1;
    final static int polar = divide + 1;
    final static int ln = polar + 1;
    final static int exp = ln + 1;
    final static int power = exp + 1;
    final static int assign = power + 1;
    final static int sqrt = assign + 1;
    final static int sin = sqrt + 1;
    final static int cos = sin + 1;
    final static int tan = cos + 1;
    final static int sininv = tan + 1;
    final static int cosinv = sininv + 1;
    final static int taninv = cosinv + 1;
    final static int sinhyp = taninv + 1;
    final static int coshyp = sinhyp + 1;
    final static int tanhyp = coshyp + 1;
    final static int asinhyp = tanhyp + 1;
    final static int acoshyp = asinhyp + 1;
    final static int atanhyp = acoshyp + 1;
    final static int conj = atanhyp + 1;
    final static int real = conj + 1;
    final static int imag = real + 1;
    final static int arg = imag + 1;
    final static int mod = arg + 1;

    final static String evalPriorityString = "(...), =, unary, ^, @, /, *, +, -"; // for user info

    // characters assigned to operations (for internal use)
    final static char atChar = '@'; // e.g. 3@4

    final static char stackChar = 130; // used for identifying if a value is in stack
    final static char logChar = stackChar + 1;
    final static char sinChar = logChar + 1;
    final static char cosChar = sinChar + 1;
    final static char tanChar = cosChar + 1;
    final static char aLogChar = tanChar + 1;
    final static char aSinChar = aLogChar + 1;
    final static char aCosChar = aSinChar + 1;
    final static char aTanChar = aCosChar + 1;
    final static char sinhChar = aTanChar + 1;
    final static char coshChar = sinhChar + 1;
    final static char tanhChar = coshChar + 1;
    final static char aSinhChar = tanhChar + 1;
    final static char aCoshChar = aSinhChar + 1;
    final static char aTanhChar = aCoshChar + 1;
    final static char conjChar = aTanhChar + 1;
    final static char realChar = conjChar + 1;
    final static char imagChar = realChar + 1;
    final static char argChar = imagChar + 1;
    final static char modChar = argChar + 1;
    final static char rootChar = modChar + 1;
    final static char piChar = rootChar + 1;
    final static char etaChar = piChar + 1;

    // string equivalent of characters
    final static String atString = new String(new char[]{atChar});
    final static String stackString = new String(new char[]{stackChar});

    final static String logString = new String(new char[]{logChar});
    final static String sinString = new String(new char[]{sinChar});
    final static String cosString = new String(new char[]{cosChar});
    final static String tanString = new String(new char[]{tanChar});
    final static String aLogString = new String(new char[]{aLogChar});
    final static String aSinString = new String(new char[]{aSinChar});
    final static String aCosString = new String(new char[]{aCosChar});
    final static String aTanString = new String(new char[]{aTanChar});
    final static String sinhString = new String(new char[]{sinhChar});
    final static String coshString = new String(new char[]{coshChar});
    final static String tanhString = new String(new char[]{tanhChar});
    final static String aSinhString = new String(new char[]{aSinhChar});
    final static String aCoshString = new String(new char[]{aCoshChar});
    final static String aTanhString = new String(new char[]{aTanhChar});
    final static String conjString = new String(new char[]{conjChar});
    final static String realString = new String(new char[]{realChar});
    final static String imagString = new String(new char[]{imagChar});
    final static String argString = new String(new char[]{argChar});
    final static String modString = new String(new char[]{modChar});
    final static String rootString = new String(new char[]{rootChar});
    final static String piString = new String(new char[]{piChar});
    final static String etString = new String(new char[]{etaChar});

    private int bracCount = 0;
    private static CN[] memory = new CN[10];
    private String remaining = "";
    public boolean eform = false;
    private static CN currentValue = null;
    private boolean evaluationMode = true;

    public CN getValue(String st) throws Exception {
        CN.stackReset();
        evaluationMode = false;
        if (evaluate(removeBrackets(substitute(st)), 0) == null) {
            throw new ArithmeticException("Syntax error");
        }
        CN.stackReset();
        evaluationMode = true;
        return currentValue = evaluate(removeBrackets(substitute(st)), 0);
    }

    private String removeBrackets(String string) throws Exception {
        String res = "";
        int l, i;
        do {
            l = string.length();
            for (i = 0; i < l; i++) {
                if (string.charAt(i) == '(') {
                    bracCount++;
                    string = string.substring(0, i) + removeBrackets(string.substring(i + 1));
                    break;
                } else {
                    if (string.charAt(i) == ')') {
                        res = evaluate(string.substring(0, i), 0).addToStack(stackString);
                        res += string.substring(i + 1);
                        if (bracCount-- > 0) {
                            return res;
                        }
                    }
                }
            }
        } while (i < l);
        res += string;
        if (bracCount > 0) {
            throw new ArithmeticException("Expected \")\"");
        }
        if (bracCount < 0) {
            throw new ArithmeticException("Expected \"(\"");
        }
        return res;
    }

    private CN evaluate(String string, int inputPriority) throws Exception {

        string = string.trim();
        if (string.equals("i")) {
            return CN.I;
        }
        if (string.equals(piString)) {
            return CN.PI;
        }
        if (string.equals(etString)) {
            return CN.ET;
        }
        if (string.equals("z")) {
            if (currentValue == null) {
                throw new ArithmeticException("variable not initialized");
            }
            return currentValue;
        }
        if (string.equals("")) {
            throw new ArithmeticException("Syntax error");
        }

        CN res = null;
        int i, l, currPriority;
        try {
            res = CN.valueOf(string, stackString);
        } catch (Exception e) {
            res = null;
        }

        do {
            if (string == null) {
                return null;
            }
            l = string.length();
            for (i = 0; i < l; i++) {
                currPriority = none;
                switch (string.charAt(i)) {
                    case 'e':
                    case 'E':
                        i++;
                        break;
                    case 'i':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case '0':
                    case ' ':
                    case 'z':
                    case piChar:
                    case etaChar:
                    case stackChar:
                    case '.':
                    case ')':
                        break;
                    case '-':
                        if (i > 0) {
                            if (string.toLowerCase().charAt(i - 1) == 'e') {
                                break;
                            }
                        }
                        if (currPriority == none) {
                            currPriority = subtract;
                        }
                        if (i == 0) {
                            res = evaluate(string.substring(1), (inputPriority > subtract) ? inputPriority : subtract).negate();
                            i = l;
                            break;
                        }
                    case '+':
                        if (currPriority == none) {
                            currPriority = add;
                        }
                        if (i == 0) {
                            res = evaluate(string.substring(1), (inputPriority > add) ? inputPriority : add);
                            i = l;
                            break;
                        }
                    case '*':
                        if (currPriority == none) {
                            currPriority = multiply;
                        }
                    case '/':
                        if (currPriority == none) {
                            currPriority = divide;
                        }
                    case '^':
                        if (currPriority == none) {
                            currPriority = power;
                        }
                    case atChar:
                        if (currPriority == none) {
                            currPriority = polar;
                        }
                        // common part below
                        if ((inputPriority >= currPriority && currPriority != power) || (inputPriority > currPriority && currPriority == power)) {
                            CN left = evaluate(string.substring(0, i), inputPriority);
                            remaining = string.substring(i);
                            return left;
                        } else {
                            CN left = evaluate(string.substring(0, i), currPriority);
                            CN right = evaluate(string.substring(i + 1), currPriority);
                            res = compute(left, right, currPriority);
                            i = l;
                        }
                        break;
                    case logChar:
                        if (currPriority == none) {
                            currPriority = ln;
                        }
                    case rootChar:
                        if (currPriority == none) {
                            currPriority = sqrt;
                        }
                    case sinChar:
                        if (currPriority == none) {
                            currPriority = sin;
                        }
                    case cosChar:
                        if (currPriority == none) {
                            currPriority = cos;
                        }
                    case tanChar:
                        if (currPriority == none) {
                            currPriority = tan;
                        }
                    case aLogChar:
                        if (currPriority == none) {
                            currPriority = exp;
                        }
                    case aSinChar:
                        if (currPriority == none) {
                            currPriority = sininv;
                        }
                    case aCosChar:
                        if (currPriority == none) {
                            currPriority = cosinv;
                        }
                    case aTanChar:
                        if (currPriority == none) {
                            currPriority = taninv;
                        }
                    case sinhChar:
                        if (currPriority == none) {
                            currPriority = sinhyp;
                        }
                    case coshChar:
                        if (currPriority == none) {
                            currPriority = coshyp;
                        }
                    case tanhChar:
                        if (currPriority == none) {
                            currPriority = tanhyp;
                        }
                    case aSinhChar:
                        if (currPriority == none) {
                            currPriority = asinhyp;
                        }
                    case aCoshChar:
                        if (currPriority == none) {
                            currPriority = acoshyp;
                        }
                    case aTanhChar:
                        if (currPriority == none) {
                            currPriority = atanhyp;
                        }
                    case conjChar:
                        if (currPriority == none) {
                            currPriority = conj;
                        }
                    case modChar:
                        if (currPriority == none) {
                            currPriority = mod;
                        }
                    case argChar:
                        if (currPriority == none) {
                            currPriority = arg;
                        }
                    case realChar:
                        if (currPriority == none) {
                            currPriority = real;
                        }
                    case imagChar:
                        if (currPriority == none) {
                            currPriority = imag;
                        }
                        // common part below
                        if (i > 0) {
                            throw new ArithmeticException("Syntax error");
                        }
                        CN right = evaluate(string.substring(i + 1), currPriority);
                        res = compute(right, currPriority);
                        i = l;
                        break;
                    case 'm':
                        if (i > 0) throw new ArithmeticException("Syntax error");
                        
                        // 3+m1
                        // 3+m1=2+3
                        // 3+m1=(2+3)
                        // 3+m18=(m2=2+3)
                        // m0=m1=1+2i     m1=(1+2i)     m2=(m1+1)+2i
                        // m4=(m3=(m2=(m0=1234^m1=5678)^(1/m1))-m0)
                        if (i == (l - 1)) throw new ArithmeticException("invalid memory usage");
                        
                        char char1 = string.charAt(i + 1),
                             char2 = 0,
                             char3 = 0;
                        String st2 = "",
                               st3 = "";
                        if (i < l - 2) {
                            st2 = string.substring(i + 2).trim();
                            if (st2.length() > 0) {
                                char2 = st2.charAt(0);
                            }
                        }
                        if (st2.length() > 1) {
                            st3 = st2.substring(1).trim();
                            if (st3.length() > 0) {
                                char3 = st3.charAt(0);
                            }
                        }

                        if (!validMem(char1) || validMem(char2)) {
                            throw new ArithmeticException("Memory variable non-existant");
                        }
                        if (char2 == '=' && char3 != 0) {
                            memory[charToInteger(char1)] = evaluate(st3, assign);
                        } else {
                            remaining = st2;
                        }
                        res = memory[charToInteger(char1)];
                        if (res == null) {
                            throw new ArithmeticException("variable not initialized");
                        }
                        i = l;
                        break;
                    default:
                        throw new ArithmeticException(string.charAt(i) + ": Invalid Operator");
                }
            }
            if (remaining.equals("")) {
                break;
            }
            string = res.addToStack(stackString) + remaining;
            remaining = "";
        } while (true);
        return res;
    }

    private static String subs(String st, String st1, String st2) {
        return st.replace(st1, st2);
    }

    private static boolean validMem(char char0) {
        switch (char0) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                return true;
            default:
                return false;
        }
    }

    private static String substitute(String st) {
        st = subs(st, "asinh", aSinhString);
        st = subs(st, "acosh", aCoshString);
        st = subs(st, "atanh", aTanhString);
        st = subs(st, "sinh", sinhString);
        st = subs(st, "cosh", coshString);
        st = subs(st, "tanh", tanhString);
        st = subs(st, "asin", aSinString);
        st = subs(st, "acos", aCosString);
        st = subs(st, "atan", aTanString);
        st = subs(st, "alog", aLogString);
        st = subs(st, "sqrt", rootString);
        st = subs(st, "conj", conjString);
        st = subs(st, "sin", sinString);
        st = subs(st, "cos", cosString);
        st = subs(st, "tan", tanString);
        st = subs(st, "log", logString);
        st = subs(st, "arg", argString);
        st = subs(st, "mod", modString);
        st = subs(st, "im", imagString);
        st = subs(st, "re", realString);
        st = subs(st, "pi", piString);
        st = subs(st, "et", etString);
        st = subs(st, "$", etString);
        return st;
    }

    private CN compute(CN left, CN right, int operation) {
        if (!evaluationMode) {
            return CN.ONE;
        }
        if (left == null || right == null) {
            throw new ArithmeticException("Syntax error");
        }
        CN result;
        switch (operation) {
            case subtract:
                result = left.subtract(right);
                break;
            case add:
                result = left.add(right);
                break;
            case divide:
                result = left.divide(right);
                break;
            case multiply:
                result = left.multiply(right);
                break;
            case power:
                result = left.pow(right);
                break;
            case polar:
                result = left.euler(right);
                break;
            default:
                throw new UnsupportedOperationException("Not yet implemented");
        }
        return result;
    }

    private CN compute(CN operand, int operation) {
        if (!evaluationMode) {
            return CN.ONE;
        }
        if (operand == null) {
            throw new ArithmeticException("Syntax error");
        }
        CN result;
        switch (operation) {
            case sqrt:
                result = operand.sqrt();
                break;
            case ln:
                result = operand.ln();
                break;
            case exp:
                result = operand.exp();
                break;
            case sin:
                result = operand.sin();
                break;
            case tan:
                result = operand.tan();
                break;
            case cos:
                result = operand.cos();
                break;
            case sininv:
                result = operand.asin();
                break;
            case cosinv:
                result = operand.acos();
                break;
            case taninv:
                result = operand.atan();
                break;
            case sinhyp:
                result = operand.sinh();
                break;
            case tanhyp:
                result = operand.tanh();
                break;
            case coshyp:
                result = operand.cosh();
                break;
            case asinhyp:
                result = operand.asinh();
                break;
            case atanhyp:
                result = operand.atanh();
                break;
            case acoshyp:
                result = operand.acosh();
                break;
            case real:
                result = operand.real();
                break;
            case imag:
                result = operand.imag();
                break;
            case conj:
                result = operand.conj();
                break;
            case arg:
                result = operand.arg();
                break;
            case mod:
                result = operand.mod();
                break;
            default:
                throw new UnsupportedOperationException("Not yet implemented");
        }
        return result;
    }

    private static int charToInteger(char char0) throws Exception {
        return Integer.valueOf(String.valueOf(char0));
    }
}
