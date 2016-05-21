package complex;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Vector;

public class BD { // wrapper class for BigDecimal
    // encapsulates BigDecimal. Contains equivalent of double functions (e.g., sin, cos, log, etc)

    private static int ROUNDING_TYPE = BigDecimal.ROUND_HALF_DOWN;

    private BigDecimal num = BigDecimal.ZERO;  // BD is wrapper for num
    private static List<BD> _cosFactors;
    private static int scale = 100;
    private static int display = 10;
    private static int internalFactor = 2;
    private static int internalScale = scale * internalFactor;

    public static final BD ZERO = zero();
    public static final BD ONE = new BD(1);
    public static final BD TWO = new BD(2);

    public static BD PI;
    public static BD ET;
    public static BD PIdiv2;
    public static BD PImul2;
    private static BigDecimal LOG10;

    private static boolean initialize() {
        initTrig();
        PI = pi();
        ET = et();
        PIdiv2 = PI.div2();
        PImul2 = PI.mul2();
        LOG10 = log10();
        return true;
    }

    private static BigDecimal log10() {
        return BigFunctionsMod.lnNewton(BigDecimal.valueOf(10.0), internalScale, ROUNDING_TYPE);
    }

    public BD(double val) {
        this.num = BigDecimal.valueOf(val);
    }

    public BD exp() {
        return new BD(BigFunctionsMod.exp(this.toBigDecimal(), internalScale, ROUNDING_TYPE));
    }

    public BD log() {
        return logNatural();
    }

    public static int getScale() {
        return scale;
    }

    public static int getDisplay() {
        return display;
    }


    private static BD zero() {
        BD out = new BD();
        out.num = BigDecimal.ZERO;
        return out;
    }

    public BigDecimal toBigDecimal() {
        return this.num;
    }

    public BigInteger toBigInteger() {
        return this.num.toBigInteger();
    }
    private static BD pi() {
        return TWO.add(ONE).sqrt().atanRestricted(ONE).multiply(6);
    }

    private static BD et() {
        return ONE.exp();
    }
    public BD mul2() {        
        return new BD(this.num.add(this.num));
    }
    public BD(BigDecimal val) {        
        this.num = val;
    }

    public BD(BigInteger val) {        
        this.num = new BigDecimal(val);
    }

    public BD(long val) {        
        this.num = BigDecimal.valueOf(val);
    }

    public BD(int val) {        
        this.num = BigDecimal.valueOf(val);
    }

    BD() {        
        this.num = BigDecimal.ZERO;
    }

    public BD(String string) {                
        this.num = new BigDecimal(string);
    }

    public BD div2() {        
        return divide(2);
    }

    public BD divide(BigInteger i) {        
        BD out = new BD();
        out.num = this.num.divide(new BigDecimal(i), internalScale, ROUNDING_TYPE);
        return out;
    }

    public BD divide(int i) {        
        BD out = new BD();
        out.num = this.num.divide(BigDecimal.valueOf(i), internalScale, ROUNDING_TYPE);
        return out;        
    }

    private BD div4() {
        return this.div2().div2();
    }

    public BD sqr() {
        
        return this.multiply(this);
    }

    public BD multiply(BD input) {        
        BD output = new BD();
        output.num = scaledValue(this.num.multiply(input.num), internalScale);                
        return output;
    }

    public BD multiply(int input) {        
        return this.multiply(new BD(input));
    }

    public BD multiply(BigInteger input) {        
        return this.multiply(new BD(input));
    }

    public BD add(BD input) {        
        BD output = new BD();
        output.num = this.num.add(input.num);
        return output;
    }

    public BD abs() {        
        BD output = new BD();
        output.num = this.num.abs();
        return output;
    }

    public BD subtract(BD input) {
        
        BD output = new BD();
        output.num = this.num.subtract(input.num);
        return output;
    }

    public int compareTo(BD input) {
        
        return this.scaledValue(scale).compareTo(input.scaledValue(scale));
    }

    private BigDecimal scaledValue(int newScale) {
        return this.num.setScale(newScale, ROUNDING_TYPE).stripTrailingZeros();
        
        
    }

    public boolean equals(BD input) {
        
        return this.compareTo(input) == 0 ? true : false;
    }

    public boolean equalZero() {
        
        return this.equals(BD.ZERO);
    }

    public static BD valueOf(int input) {
        
        BD output = new BD();
        output.num = BigDecimal.valueOf(input);
        return output;
    }

    public static BD valueOf(double input) {
        
        return new BD(BigDecimal.valueOf(input));
    }

    public BD negate() {
        
        BD output = new BD();
        output.num = this.num.negate();
        return output;
    }

    public BD pow(int p) {        
        BD output = new BD();
        output.num = scaledValue(this.num.pow(p), internalScale);
        return output;
    }

    public BD divide(BD input) {
        
        if (input.equalZero()) {
            throw new ArithmeticException("Division by zero");
        }
        BD output = new BD();
        output.num = this.num.divide(input.num, internalScale, ROUNDING_TYPE);
        return output;
    }


    public static void initScale(int scaleBits) {
        
        if (scaleBits < 0 || scaleBits > 2048) {
            throw new ArithmeticException("illegal scale");
        }
        scale = scaleBits;
        internalScale = internalFactor * scale;
        if (display > scale) {
            display = scale;
        }
        initialize();
    }

    public static void initDisp(int displayBits) {
        if (displayBits < 0 || displayBits > scale) {
            throw new ArithmeticException("illegal display setting");
        }
        display = displayBits;
    }

    @Override
    public String toString(){
        BigDecimal value = this.scaledValue(internalScale);
        if (value.equals(BigDecimal.ZERO)) return "0";
        return this.num.toString();
    }

    private static void initTrig() {
        _cosFactors = new Vector<BD>();
        BD factor = null;
        BD divWith = BD.TWO;
        BD inc = BD.TWO;
        do {
            factor = ONE.divide(divWith);
            _cosFactors.add(factor);
            inc = inc.add(ONE);
            divWith = divWith.multiply(inc);
            inc = inc.add(ONE);
            divWith = divWith.multiply(inc);
        } while (factor.scaledValue(internalScale).compareTo(BigDecimal.ZERO) > 0);
        
    }

    private BD reduceTrigRange() {
        return this.subtract(PImul2.multiply(this.divide(PImul2).toBigInteger()));
    }

    public BD cos() {
        if (this.equalZero()) {
            return ONE;
        }
        BD res = ONE;
        if ((this.abs()).compareTo(PImul2) > 0) {
            return (this.reduceTrigRange()).cos();
        }
        if (this.abs().compareTo(ONE) > 0) {
            BD temp = ((this.div4()).cos()).sqr();
            return ((temp.sqr().subtract(temp)).multiply(8)).add(ONE);
        }
        BD xn = this.sqr();
        for (int i = 0; i < _cosFactors.size(); i++) {
            BD factor = (BD) _cosFactors.get(i);
            factor = factor.multiply(xn);
            if (i % 2 == 0) {
                factor = factor.negate();
            }
            res = res.add(factor);
            xn = xn.multiply(this);
            xn = xn.multiply(this);
        }
        return res;
    }

    public BD sin() {
        if (this.equalZero()) {
            return ZERO;

        }
        if ((this.abs()).compareTo(PImul2) > 0) {
            return (this.reduceTrigRange()).sin();
        }
        return (PIdiv2.subtract(this)).cos();
    }

    public String format() {
        BigDecimal temp = scaledValue(display);        
        if (temp.abs().compareTo(BigDecimal.ZERO) <= 0) {            
            return "0";
        }            
        return temp.toPlainString();
    }

    private static BigDecimal scaledValue(BigDecimal b, int newScale) {
        return b.divide(BigDecimal.ONE, newScale, ROUNDING_TYPE);
    }

    public BD sqrt() {        
        return new BD(BigFunctionsMod.sqrt(this.toBigDecimal(), internalScale));        
    }

    public static BD atan2(BD adj, BD opp) {
        return adj.atan2(opp);
    }

    private BD atanRestricted(BD opp) {
        BD adj = this;
        BD output = new BD();
        if (opp.equalZero()) {
            return ZERO;
        }
        int toSwap = adj.abs().scaledValue(internalScale).compareTo(opp.abs().scaledValue(internalScale));
        switch (toSwap) {
            case 0:
            case -1:
                return null;
            case +1:
                if (adj.num.equals(BigDecimal.ZERO)) {
                    return null;
                } else {
                    output = new BD(BigFunctionsMod.arctan(opp.num.divide(adj.num, internalScale, ROUNDING_TYPE), internalScale, ROUNDING_TYPE));
                }
                break;
            default:
        }
        
        return output;
    }

    private static BD atan(BD x) {
        BD output = new BD();
        int toInvert = x.abs().compareTo(BD.ONE); 
        if (x.equalZero()) {
            return ZERO;            
        }
        boolean isHalf = false;
        switch (toInvert) {
            case 0:
                return PI.div4().multiply(x.signnum());
            case +1:
                x = BD.ONE.divide(x);
            case -1:
                if (x.abs().toBigDecimal().compareTo(BigDecimal.valueOf(0.9)) == 1) {
                    isHalf = true;
                    x = x.divide(ONE.add((ONE.add(x.sqr())).sqrt()));
                }
                //System.out.println("here");
        
                output = new BD(BigFunctionsMod.arctan(x.toBigDecimal(), internalScale, ROUNDING_TYPE));
                break;
            default:
        }
        if (isHalf) {
            output = output.mul2();
        }
        if (toInvert == 1) {
            output = PIdiv2.multiply(x.signnum()).subtract(output);
        }
        return output;
    }

    private BD atan2(BD opp) {
        BD adj = this;
        int signAdj = adj.signnum();
        int signOpp = opp.signnum();
        if (opp.equalZero()) {
            return (signAdj == -1) ? PI : ZERO;
        }
        if (adj.equalZero()) {
            return PIdiv2.multiply(signOpp);
        }
        BD temp = atan(opp.divide(adj));
        if (signAdj == -1) {
            return temp.add(PI);
        }
        return temp;
    }

    public int signnum() {
        return this.num.signum();
    }

    private BD logNatural() {
        if (this.compareTo(ZERO) <= 0) {
            throw new ArithmeticException("Log: illegal operation"); 
        }
        int digits = (int)
        java.lang.Math.floor(this.toBigInteger().bitLength() / 3.3219280948873623478703194294894);
        
        BigDecimal temp = scaledValue(this.num.movePointLeft(digits), internalScale);
        return new BD(BigFunctionsMod.lnNewton(temp, internalScale, ROUNDING_TYPE).add(LOG10.multiply(BigDecimal.valueOf(digits))));
    }

    public BD pow(BD y) {
        BigInteger x = y.num.toBigInteger();
        BigDecimal fracPart = y.num.subtract(new BigDecimal(x));
        BigDecimal intPowr = BigFunctionsMod.intPower(num, x.longValue(), internalScale, ROUNDING_TYPE);
        BD thisLog = this.log();
        BigDecimal fracPowr = BigFunctionsMod.exp(thisLog.toBigDecimal().multiply(fracPart), internalScale, ROUNDING_TYPE);
        return new BD(BD.scaledValue(intPowr.multiply(fracPowr), internalScale));
    }    
}

