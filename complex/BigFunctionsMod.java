package complex;

// Modified version of BigFunctions library from the book
// Java number cruncher: the Java programmer's guide to numerical computing
// Modified by Amitabh. Sections modified: arctanTaylor, expTaylor, lnNewton.

import java.math.BigInteger;
import java.math.BigDecimal;

/**
 * Several useful BigDecimal mathematical functions.
 */
public final class BigFunctionsMod {

    /**
     * Compute x^exponent to a given scale.  Uses the same
     * algorithm as class numbercruncher.mathutils.IntPower.
     * @param x the value x
     * @param exponent the exponent value
     * @param scale the desired scale of the result
     * @return the result value
     */
    public static BigDecimal intPower(BigDecimal x, long exponent,
            int scale, int ROUNDING_TYPE) {
        // If the exponent is negative, compute 1/(x^-exponent).
        if (exponent < 0) {
            return BigDecimal.valueOf(1).divide(intPower(x, -exponent, scale, ROUNDING_TYPE), scale,
                    ROUNDING_TYPE);
        }

        BigDecimal power = BigDecimal.valueOf(1);

        // Loop to compute value^exponent.

        while (exponent > 0) {
            // Is the rightmost bit a 1?
            if ((exponent & 1) == 1) {
                power = power.multiply(x).setScale(scale, ROUNDING_TYPE);
            }

            // Square x and shift exponent 1 bit to the right.
            x = x.multiply(x).setScale(scale, ROUNDING_TYPE);
            exponent >>= 1;

            Thread.yield();
        }
        return power;
    }

    /**
     * Compute the integral root of x to a given scale, x >= 0.
     * Use Newton's algorithm.
     * @param x the value of x
     * @param index the integral root value
     * @param scale the desired scale of the result
     * @return the result value
     */
    public static BigDecimal intRoot(BigDecimal x, long index,
            int scale, int ROUNDING_TYPE) {
        // Check that x >= 0.
        if (x.signum() < 0) {
            throw new IllegalArgumentException("x < 0");
        }

        // if |x^index| is more than 10^10000 then use some other trick.
        //BigInteger length = BigInteger.valueOf(x.toBigInteger().abs().bitLength()).multiply(BigInteger.valueOf(index));
        long length = x.toBigInteger().bitLength();
        if (length * index > 500000) {
            Tuple y = getApproxRoot(x.abs(), index, length, scale, ROUNDING_TYPE);
            x = x.divide(y.power, scale, ROUNDING_TYPE);
            return intRoot(x, index, scale, ROUNDING_TYPE).multiply(y.base);
        }

        int sp1 = scale + 1;
        BigDecimal n = x;
        BigDecimal i = BigDecimal.valueOf(index);
        BigDecimal im1 = BigDecimal.valueOf(index - 1);
        BigDecimal tolerance = BigDecimal.valueOf(5).movePointLeft(sp1);
        BigDecimal xPrev;

        // The initial approximation is x/index.
        x = x.divide(i, scale, ROUNDING_TYPE);

        // Loop until the approximations converge
        // (two successive approximations are equal after rounding).
        do {

            // x^(index-1)
            BigDecimal xToIm1 = intPower(x, index - 1, sp1, ROUNDING_TYPE);

            // x^index
            BigDecimal xToI =
                    x.multiply(xToIm1).setScale(sp1, ROUNDING_TYPE);

            // n + (index-1)*(x^index)
            BigDecimal numerator =
                    n.add(im1.multiply(xToI)).setScale(sp1, ROUNDING_TYPE);

            // (index*(x^(index-1))
            BigDecimal denominator =
                    i.multiply(xToIm1).setScale(sp1, ROUNDING_TYPE);

            // x = (n + (index-1)*(x^index)) / (index*(x^(index-1)))
            xPrev = x;
            x = numerator.divide(denominator, sp1, BigDecimal.ROUND_DOWN);

            Thread.yield();
        } while (x.subtract(xPrev).abs().compareTo(tolerance) > 0);
        return x;
    }

    /**
     * Compute e^x to a given scale.
     * Break x into its whole and fraction parts and
     * compute (e^(1 + fraction/whole))^whole using Taylor's formula.
     * @param x the value of x
     * @param scale the desired scale of the result
     * @return the result value
     */
    public static BigDecimal exp(BigDecimal x, int scale, int ROUNDING_TYPE) {
        //        System.out.println("     Here in BigFunctionsAmit.exp: ->");// x = "+ x+ ", powr = "+sp1);

        // e^0 = 1
        if (x.signum() == 0) {
            //            System.out.println("     Exiting BigFunctionsAmit.exp: <-");// x = "+ x+ ", powr = "+sp1);
            return BigDecimal.valueOf(1);
        } // If x is negative, return 1/(e^-x).
        else if (x.signum() == -1) {

            BigDecimal temp = BigDecimal.valueOf(1).divide(exp(x.negate(), scale, ROUNDING_TYPE), scale,
                    ROUNDING_TYPE);
            //            System.out.println("     Exiting BigFunctionsAmit.exp: <-");// x = "+ x+ ", powr = "+sp1);
            return temp;
        }

        // Compute the whole part of x.
        BigDecimal xWhole = x.setScale(0, BigDecimal.ROUND_DOWN);

        // If there isn't a whole part, compute and return e^x.
        if (xWhole.signum() == 0) {
            return expTaylor(x, scale, ROUNDING_TYPE);
        }

        // Compute the fraction part of x.
        BigDecimal xFraction = x.subtract(xWhole);

        // z = 1 + fraction/whole
        BigDecimal z = BigDecimal.valueOf(1).add(xFraction.divide(
                xWhole, scale,
                ROUNDING_TYPE));

        // t = e^z
        BigDecimal t = expTaylor(z, scale, ROUNDING_TYPE);

        BigDecimal maxLong = BigDecimal.valueOf(Long.MAX_VALUE);
        BigDecimal result = BigDecimal.valueOf(1);

        // Compute and return t^whole using intPower().
        // If whole > Long.MAX_VALUE, then first compute products
        // of e^Long.MAX_VALUE.
        while (xWhole.compareTo(maxLong) >= 0) {
            result = result.multiply(
                    intPower(t, Long.MAX_VALUE, scale, ROUNDING_TYPE)).setScale(scale, ROUNDING_TYPE);
            xWhole = xWhole.subtract(maxLong);

            Thread.yield();
        }
        return result.multiply(intPower(t, xWhole.longValue(), scale, ROUNDING_TYPE)).setScale(scale, ROUNDING_TYPE);
    }

    /**
     * Compute e^x to a given scale by the Taylor series.
     * @param x the value of x
     * @param scale the desired scale of the result
     * @return the result value
     */
    private static BigDecimal expTaylor(BigDecimal x, int scale, int ROUNDING_TYPE) {
        BigDecimal factorial = BigDecimal.valueOf(1);
        BigDecimal xPower = x;
        BigDecimal sumPrev;

        // 1 + x
        BigDecimal sum = x.add(BigDecimal.valueOf(1));

        // Loop until the sums converge
        // (two successive sums are equal after rounding).
        int i = 2;
        do {
            // x^i
            xPower = xPower.multiply(x).setScale(scale, ROUNDING_TYPE);

            // i!
            factorial = factorial.multiply(BigDecimal.valueOf(i));

            // x^i/i!
            BigDecimal term = xPower.divide(factorial, scale,
                    ROUNDING_TYPE);

            // sum = sum + x^i/i!
            sumPrev = sum;
            sum = sum.add(term);
            //System.out.println("Here in BigFunctionsAmit.expTaylor: ");
            ++i;
            Thread.yield();
        } while (sum.compareTo(sumPrev) != 0);

        return sum;
    }

    /**
     * Compute the natural logarithm of x to a given scale, x > 0.
     */
    public static BigDecimal ln(BigDecimal x, int scale, int ROUNDING_TYPE) {

        // Check that x > 0.
        if (x.signum() <= 0) {
            throw new IllegalArgumentException("x <= 0");
        }

        // The number of digits to the left of the decimal point.
        int magnitude = x.toString().length() - x.scale() - 1;

        if (magnitude < 3) {
            BigDecimal temp = lnNewton(x, scale, ROUNDING_TYPE);

            return temp; //lnNewton(x, scale);
        } // Compute magnitude*ln(x^(1/magnitude)).
        else {
            // x^(1/magnitude)
            BigDecimal root = intRoot(x, magnitude, scale, ROUNDING_TYPE);

            // ln(x^(1/magnitude))
            BigDecimal lnRoot = lnNewton(root, scale, ROUNDING_TYPE);

            // magnitude*ln(x^(1/magnitude))
            return BigDecimal.valueOf(magnitude).multiply(lnRoot).setScale(scale, ROUNDING_TYPE);
        }
    }

    /**
     * Compute the natural logarithm of x to a given scale, x > 0.
     * Use Newton's algorithm.
     */
    public static BigDecimal lnNewton(BigDecimal x, int scale, int ROUNDING_TYPE) {
        int sp1 = scale + 1;
        BigDecimal n = x;
        BigDecimal term;

        // Convergence tolerance = 5*(10^-(scale+1))
        BigDecimal tolerance = BigDecimal.valueOf(5).movePointLeft(sp1);

        // Loop until the approximations converge
        // (two successive approximations are within the tolerance).
        do {
            // e^x
            BigDecimal eToX = exp(x, sp1, ROUNDING_TYPE);

            // (e^x - n)/e^x
            term = eToX.subtract(n).divide(eToX, sp1, BigDecimal.ROUND_DOWN);

            // x - (e^x - n)/e^x
            x = x.subtract(term);

            Thread.yield();
        } while (term.compareTo(tolerance) > 0);

        return x.setScale(scale, ROUNDING_TYPE);
    }

    /**
     * Compute the arctangent of x to a given scale, |x| < 1
     * @param x the value of x
     * @param scale the desired scale of the result
     * @return the result value
     */
    public static BigDecimal arctan(BigDecimal x, int scale, int ROUNDING_TYPE) {
        // Check that |x| < 1.
        if (x.abs().compareTo(BigDecimal.valueOf(1)) >= 0) {
            throw new IllegalArgumentException("|x| >= 1");
        }

        // If x is negative, return -arctan(-x).
        if (x.signum() == -1) {
            return arctan(x.negate(), scale, ROUNDING_TYPE).negate();
        } else {
            return arctanTaylor(x, scale, ROUNDING_TYPE);
        }
    }

    /**
     * Compute the arctangent of x to a given scale
     * by the Taylor series, |x| < 1
     * @param x the value of x
     * @param scale the desired scale of the result
     * @return the result value
     */
    private static BigDecimal arctanTaylor(BigDecimal x, int scale, int ROUNDING_TYPE) {
        int sp1 = scale + 1;
        int i = 3;
        boolean addFlag = false;

        BigDecimal power = x;
        BigDecimal sum = x;
        BigDecimal term;

        // Convergence tolerance = 5*(10^-(scale+1))
        BigDecimal tolerance = BigDecimal.valueOf(5).movePointLeft(sp1);
        // Loop until the approximations converge
        // (two successive approximations are within the tolerance).
        do {
            // x^i
            power = power.multiply(x).multiply(x).setScale(sp1, ROUNDING_TYPE);

            // (x^i)/i
            term = power.divide(BigDecimal.valueOf(i), sp1,
                    ROUNDING_TYPE);

            // sum = sum +- (x^i)/i
            sum = addFlag ? sum.add(term)
                    : sum.subtract(term);

            i += 2;
            addFlag = !addFlag;
            Thread.yield();

        } while (term.compareTo(tolerance) > 0);
        return sum;
    }

    /**
     * Compute the square root of x to a given scale, x >= 0.
     * Use Newton's algorithm.
     * @param x the value of x
     * @param scale the desired scale of the result
     * @return the result value
     */
    public static BigDecimal sqrt(BigDecimal x, int sc) {
        if (x.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;//System.out.println("here too");;
        }        // Check that x >= 0.
        if (x.signum() < 0) {
            throw new IllegalArgumentException("x < 0");
        }

        // n = x*(10^(2*scale))
        BigInteger n = x.movePointRight(sc << 1).toBigInteger();

        // The first approximation is the upper half of n.
        int bits = (n.bitLength() + 1) >> 1;
        BigInteger ix = n.shiftRight(bits);
        BigInteger ixPrev;

        // Loop until the approximations converge
        // (two successive approximations are equal after rounding).
        do {
            ixPrev = ix;
            if (ix.equals(BigInteger.ZERO)) {
                break;
            }
            // x = (x + n/x)/2
            ix = ix.add(n.divide(ix)).shiftRight(1);
            Thread.yield();

        } while ((ixPrev.subtract(ix)).abs().compareTo(BigInteger.ONE) > 0 //ix.compareTo(ixPrev) != 0
                );

        return new BigDecimal(ix, sc);
    }

    private static Tuple getApproxRoot(BigDecimal x, long index, long length, int scale, int ROUNDING_TYPE) {
        // returns random y such that y^index <= x and y^(index+1) > x
        BigDecimal power, root;
        BigDecimal nextPower = BigDecimal.ZERO;
        if (index == 0) {
            throw new ArithmeticException("Illegal operation: zeroth root.");
        }

        root = intPower(BigDecimal.valueOf(2.0), (long) (length / index), scale, ROUNDING_TYPE);

        BigDecimal low = BigDecimal.ONE;
        BigDecimal high = root;
        do {
            power = intPower(root, index, scale, ROUNDING_TYPE);
            nextPower = power.multiply(intPower(root, 2 + (int) java.lang.Math.sqrt(index), scale, ROUNDING_TYPE));//.multiply(root).multiply(root);

            int grThan = x.compareTo(power);
            if (grThan == 0) {
                return new Tuple(root, power);
            }
            int lessThan = x.compareTo(nextPower);

            if (grThan > 0 && lessThan < 0) {
                return new Tuple(root, power);
            }
            if (grThan < 0) {
                high = root;
                root = (root.add(low)).divide(BigDecimal.valueOf(2), scale, ROUNDING_TYPE);
            }
            if (lessThan > 0) {
                low = root;
                high = high.add(high);
                root = (root.add(high)).divide(BigDecimal.valueOf(2), scale, ROUNDING_TYPE);
            }
        } while (true);
    }

    private static class Tuple {

        BigDecimal base;
        BigDecimal power;

        Tuple() {
            this.base = BigDecimal.ONE;
            this.power = BigDecimal.ONE;
        }

        Tuple(BigDecimal b, BigDecimal p) {
            this();
            this.base = b;
            this.power = p;
        }
    }

    public static BigDecimal ln2(BigDecimal x, int scale, int ROUNDING_TYPE) {
        return lnNewton(x, scale, ROUNDING_TYPE);
    }
}


