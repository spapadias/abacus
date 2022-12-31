package BCD;

class Common {
    public static int[] parseEdge(String line, String delim) {
        String[] tokens = line.split(delim);

        int src = Integer.valueOf(tokens[0]);
        int dst = Integer.valueOf(tokens[1]);
        int sign = Integer.valueOf(tokens[2]);

        return new int[]{src, dst, sign};
    }

    public static class Quadruple<A, B, C, D> {
        private A first;
        private B second;
        private C third;
        private D fourth;

        public Quadruple(A first, B second, C third, D fourth) {
            this.first = first;
            this.second = second;
            this.third = third;
            this.fourth = fourth;
        }

        public A getFirst() {
            return first;
        }

        public B getSecond() {
            return second;
        }

        public C getThird() {
            return third;
        }

        public D getFourth() {
            return fourth;
        }
    }

}
