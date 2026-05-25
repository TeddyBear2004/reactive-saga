package hamburg.engelmann.saga.di.internal;

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Analyses a target type and the saga output history to produce a field → step index mapping.
 */
public class SagaInputMapper {

    public record OutputLocator(int stepIndex, Method accessor) { }

    public static Map<String, OutputLocator> buildMapping(Class<?> target, List<Class<?>> history) {
        Map<String, OutputLocator> map = new HashMap<>();
        if (target.isInterface()) {
            for (Method m : target.getMethods()) {
                map.put(m.getName(), findLocator(m.getName(), m.getReturnType(), history, target));
            }
        } else if (target.isRecord()) {
            for (RecordComponent rc : target.getRecordComponents()) {
                map.put(rc.getName(), findLocator(rc.getName(), rc.getType(), history, target));
            }
        }
        return map;
    }

    private static OutputLocator findLocator(String targetName, Class<?> targetType,
                                              List<Class<?>> history, Class<?> requestingType) {
        String getter = "get" + capitalize(targetName);
        String isGetter = "is" + capitalize(targetName);
        List<MatchResult> matches = new ArrayList<>();
        Set<Method> seen = Collections.newSetFromMap(new IdentityHashMap<>());

        for (int i = history.size() - 1; i >= 0; i--) {
            Class<?> cls = history.get(i);
            if (targetType.isAssignableFrom(cls)) {
                matches.add(new MatchResult(new OutputLocator(i, null), "Direct: " + cls.getSimpleName()));
            }
            if (cls.isRecord()) {
                for (RecordComponent rc : cls.getRecordComponents()) {
                    if (rc.getName().equals(targetName) && targetType.isAssignableFrom(rc.getType())) {
                        Method accessor = rc.getAccessor();
                        accessor.setAccessible(true);
                        matches.add(new MatchResult(new OutputLocator(i, accessor),
                                                    "Record '" + rc.getName() + "' in " + cls.getSimpleName()));
                        seen.add(accessor);
                    }
                }
            } else {
                for (Method m : cls.getMethods()) {
                    if (seen.contains(m) || m.getParameterCount() != 0) continue;
                    if (!targetType.isAssignableFrom(m.getReturnType())) continue;
                    String mn = m.getName();
                    boolean match = mn.equals(targetName) || mn.equals(getter)
                                    || ((targetType == boolean.class || targetType == Boolean.class) && mn.equals(isGetter));
                    if (match) {
                        m.setAccessible(true);
                        matches.add(new MatchResult(new OutputLocator(i, m), "Method '" + mn + "()' in " + cls.getSimpleName()));
                    }
                }
            }
        }

        if (matches.isEmpty()) {
            String available = history.isEmpty() ? "<no previous steps>"
                    : history.stream().map(c -> {
                        // Use binary name to identify StepResult$EmptyOutput without a compile-time dependency.
                        if ("hamburg.engelmann.saga.step.StepResult$EmptyOutput".equals(c.getName())) return "";
                        List<String> props = c.isRecord()
                                ? Arrays.stream(c.getRecordComponents()).map(rc -> rc.getType().getSimpleName() + " " + rc.getName()).toList()
                                : Arrays.stream(c.getMethods()).filter(m -> m.getParameterCount() == 0 && !m.getName().equals("getClass") && !m.getReturnType().equals(void.class)).map(m -> m.getName() + ": " + m.getReturnType().getSimpleName()).toList();
                        return c.getSimpleName() + " {" + String.join(", ", props) + "}";
                    }).filter(s -> !s.isEmpty()).collect(Collectors.joining("\n  "));
            throw new IllegalStateException(String.format(
                    "Cannot find source for '%s %s' required by '%s'.%nAvailable:%n  %s",
                    targetType.getSimpleName(), targetName, requestingType.getCanonicalName(), available));
        }
        if (matches.size() > 1) {
            throw new IllegalStateException(String.format(
                    "Ambiguous mapping for '%s %s'. Use unique field names.", targetType.getSimpleName(), targetName));
        }
        return matches.getFirst().locator();
    }

    private static String capitalize(String s) {
        return (s == null || s.isEmpty()) ? s : s.substring(0, 1).toUpperCase() + s.substring(1);
    }

    private record MatchResult(OutputLocator locator, String description) { }

}
